using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace SampleCalculation.Redis.BLL
{
    public class RedisService : IRedisService
    {
        private readonly ILogger _logger;

        private readonly Lazy<ConnectionMultiplexer> _lazyConnection;

        public RedisService(IConfiguration configuration, ILogger<RedisService> logger)
        {
            var connectionString = configuration.GetValue<string>("RedisServer:ConnectionString");
            _lazyConnection = new Lazy<ConnectionMultiplexer>(() => ConnectionMultiplexer.Connect(connectionString));
            _logger = logger;
        }

        public ConnectionMultiplexer Connection => _lazyConnection.Value;

        private IDatabase RedisDb => Connection.GetDatabase();

        private List<IServer> RedisServers
        {
            get
            {
                var endpoints = Connection.GetEndPoints();
                var server = new List<IServer>();

                foreach (var endpoint in endpoints)
                {
                    server.Add(Connection.GetServer(endpoint));
                }

                return server;
            }
        }

        public async Task<bool> DeleteAsync(string key)
        {
            try
            {
                var stringValue = await RedisDb.KeyDeleteAsync(key);
                return true;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, ex.Message);
                return false;
            }
        }

        public async Task<bool> DeleteByPatternAsync(string pattern)
        {
            try
            {
                foreach (var redisServer in RedisServers)
                {
                    var keys = redisServer.Keys(pattern: pattern).ToArray();
                    await RedisDb.KeyDeleteAsync(keys);
                }

                return true;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, ex.Message);
                return false;
            }
        }

        public async Task<T> GetAsync<T>(string key)
        {

            try
            {
                var stringValue = await RedisDb.StringGetAsync(key);
                if (string.IsNullOrEmpty(stringValue))
                {
                    return default(T);
                }
                else
                {
                    var objectValue = JsonConvert.DeserializeObject<T>(stringValue);
                    return objectValue;
                }

            }
            catch (Exception ex)
            {
                _logger.LogError(ex, ex.Message);
                return default(T);
            }
        }

        public async Task<IEnumerable<T>> GetAsync<T>(string[] keys)
        {
            try
            {
                var redisKeys = keys.Select(x => new RedisKey(x)).ToArray();
                var values = await RedisDb.StringGetAsync(redisKeys);

                if (values == null || values.Length == 0)
                {
                    return default(IEnumerable<T>);
                }
                else
                {
                    var objectValue = values.Select(x => JsonConvert.DeserializeObject<T>(x));
                    return objectValue;
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, ex.Message);
                return default(IEnumerable<T>);
            }
        }

        public async Task SaveAsync(string key, object value)
        {
            try
            {
                var stringValue = JsonConvert.SerializeObject(value,
                new JsonSerializerSettings()
                {
                    ReferenceLoopHandling = ReferenceLoopHandling.Ignore
                });
                await RedisDb.StringSetAsync(key, stringValue);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, ex.Message);
                return;
            }
        }

        public async Task<RedisValue[]> SetCombine(SetOperation opr, string[] keys)
        {
            try
            {
                var redisKeys = keys.Select(x => new RedisKey(x)).ToArray();
                var result = await RedisDb.SetCombineAsync(opr, redisKeys);
                return result;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, ex.Message);
                return null;
            }
        }

        public async Task SaveBatchAsync(KeyValuePair<RedisKey, RedisValue>[] data)
        {
            try
            {
                await RedisDb.StringSetAsync(data);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, ex.Message);
                return;
            }
        }


        public async Task<string[]> GetSetMembersAsync(string key)
        {
            try
            {
                var setMember = await RedisDb.SetMembersAsync(key);

                if (null != setMember)
                {
                    return setMember.ToStringArray();
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, ex.Message);
            }
            return null;
        }

        public async Task AddToSetAsync(string key, string value)
        {
            try
            {
                await RedisDb.SetAddAsync(key, value);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, ex.Message);
                return;
            }
        }

        public async Task AddToSetAsync(string key, RedisValue[] value)
        {
            try
            {
                await RedisDb.SetAddAsync(key, value);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, ex.Message);
                return;
            }
        }
    }
}
