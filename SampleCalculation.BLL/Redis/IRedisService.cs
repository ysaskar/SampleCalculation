using StackExchange.Redis;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace SampleCalculation.Redis.BLL
{
    public interface IRedisService
    {
        Task SaveAsync(string key, object value);
        Task AddToSetAsync(string key, string value);
        Task<T> GetAsync<T>(string key);
        Task<IEnumerable<T>> GetAsync<T>(string[] keys);
        Task<string[]> GetSetMembersAsync(string key);
        Task<bool> DeleteAsync(string key);
        Task<bool> DeleteByPatternAsync(string pattern);
        Task AddToSetAsync(string key, RedisValue[] value);
        Task SaveBatchAsync(KeyValuePair<RedisKey, RedisValue>[] data);
        Task<RedisValue[]> SetCombine(SetOperation opr, string[] keys);
    }
}
