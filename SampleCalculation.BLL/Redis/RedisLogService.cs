using SampleCalculation.BLL.Dto;
using SampleCalculation.BLL.Enum;
using SampleCalculation.Redis.BLL;
using System;
using System.Threading.Tasks;

namespace SampleCalculation.BLL.Redis
{
    public class RedisLogService : IRedisLogService
    {
        private readonly IRedisService _redis;

        public RedisLogService(IRedisService redis)
        {
            _redis = redis;
        }

        public async Task LogBegin(BaseMessageDto dto, string logKey, DateTime begin)
        {
            await LogToRedis(dto, logKey, EnumStatus.Begin, begin);
        }

        public async Task LogError(BaseMessageDto dto, string logKey, Exception ex)
        {
            await LogToRedis(dto, logKey, EnumStatus.Error, null, $"Error, {ex}");
        }

        public async Task LogFailed(BaseMessageDto dto, string logKey, string msg)
        {
            await LogToRedis(dto, logKey, EnumStatus.Failed, null, msg);
        }

        public async Task LogFinish(BaseMessageDto dto, string logKey)
        {
            await LogToRedis(dto, logKey, EnumStatus.Completed, null);
        }

        public async Task LogToRedis(BaseMessageDto dto, string logKey, EnumStatus status, DateTime? begin, string msg = null)
        {
            begin ??= DateTime.UtcNow;

            var logBegin = new BaseMessageDto()
            {
                TrxId = dto.TrxId,
                Activity = dto.Activity,
                Timestamp = begin.Value,
                Status = status,
                Message = msg
            };

            await _redis.SaveAsync(logKey, logBegin);
        }

    }
}
