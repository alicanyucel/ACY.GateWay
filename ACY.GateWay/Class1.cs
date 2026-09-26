using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using System.Collections.Concurrent;

namespace NetBridge.Core;

public class GatewayConfig
{
    public Route[] Routes { get; set; }
    public string BaseUrl { get; set; }
}

public class Route
{
    public string UpstreamPathTemplate { get; set; }
    public string DownstreamPathTemplate { get; set; }
    public string DownstreamHost { get; set; }
    public int DownstreamPort { get; set; }
    public bool EnableRateLimiting { get; set; }
    public int LimitPerSecond { get; set; }
    public bool RequireAuth { get; set; }
    public bool EnableCaching { get; set; }
    public int CacheSeconds { get; set; }
    public bool EnableQoS { get; set; }
    public int RetryCount { get; set; }
}

public class NetBridgeGateway
{
    private readonly GatewayConfig _config;
    private readonly ConcurrentDictionary<string, int> _rateCounter = new();
    private readonly ConcurrentDictionary<string, (string response, DateTime expiry)> _cache = new();

    public NetBridgeGateway(GatewayConfig config)
    {
        _config = config;
    }

    public async Task HandleRequest(HttpContext context)
    {
        var path = context.Request.Path.Value;
        var route = MatchRoute(path);

        if (route == null)
        {
            context.Response.StatusCode = 404;
            await context.Response.WriteAsync("Route not found");
            return;
        }

        // 1. Auth kontrolü
        if (route.RequireAuth && !ValidateJwt(context))
        {
            context.Response.StatusCode = 401;
            await context.Response.WriteAsync("Unauthorized");
            return;
        }

        // 2. Rate limit
        if (route.EnableRateLimiting && !AllowRequest(route.UpstreamPathTemplate, route.LimitPerSecond))
        {
            context.Response.StatusCode = 429;
            await context.Response.WriteAsync("Too Many Requests");
            return;
        }

        // 3. Cache kontrolü
        if (route.EnableCaching && _cache.TryGetValue(path, out var cached) && cached.expiry > DateTime.Now)
        {
            await context.Response.WriteAsync(cached.response);
            return;
        }

        // 4. Service discovery (basit: host + port)
        var downstreamUrl = $"http://{route.DownstreamHost}:{route.DownstreamPort}{route.DownstreamPathTemplate}";

        // 5. QoS (retry + circuit breaker)
        string response = await ForwardWithQoS(downstreamUrl, route);

        // 6. Loglama
        Console.WriteLine($"[{DateTime.Now}] {path} -> {downstreamUrl}");

        // 7. Cache yaz
        if (route.EnableCaching)
            _cache[path] = (response, DateTime.Now.AddSeconds(route.CacheSeconds));

        await context.Response.WriteAsync(response);
    }

    private Route MatchRoute(string path)
    {
        foreach (var route in _config.Routes)
        {
            if (path.StartsWith(route.UpstreamPathTemplate))
                return route;
        }
        return null;
    }

    private bool ValidateJwt(HttpContext context)
    {
        var authHeader = context.Request.Headers["Authorization"].ToString();
        return !string.IsNullOrEmpty(authHeader) && authHeader.StartsWith("Bearer ");
    }

    private bool AllowRequest(string key, int limit)
    {
        var count = _rateCounter.AddOrUpdate(key, 1, (_, old) => old + 1);
        if (count > limit) return false;
        return true;
    }

    private async Task<string> ForwardWithQoS(string downstreamUrl, Route route)
    {
        using var client = new HttpClient();
        int retries = route.EnableQoS ? route.RetryCount : 1;

        for (int i = 0; i < retries; i++)
        {
            try
            {
                return await client.GetStringAsync(downstreamUrl);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Retry {i + 1}/{retries} failed: {ex.Message}");
                if (i == retries - 1) throw;
            }
        }
        return "Service unavailable";
    }
}

public static class GatewayExtensions
{
    public static IApplicationBuilder UseNetBridge(this IApplicationBuilder app, GatewayConfig config)
    {
        var gateway = new NetBridgeGateway(config);
        return app.Use(async (context, next) =>
        {
            await gateway.HandleRequest(context);
        });
    }
}
