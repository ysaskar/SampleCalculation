using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using SampleCalculation.BLL.Dto;
using SampleCalculation.BLL.Enum;
using SampleCalculation.BLL.Kafka;
using SampleCalculation.BLL.Redis;
using SampleCalculation.Redis.BLL;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace SampleCalculation.BLL
{
    public class ValidationPartTwo : IConsumeProcess
    {
        private readonly ILogger _logger;
        private readonly IRedisService _redis;
        private readonly IKafkaSender _sender;
        private readonly IConfiguration _config;
        private readonly IRedisLogService _redisLog;

        public ValidationPartTwo(ILogger<ValidationPartTwo> logger, IConfiguration config, IRedisService redis, IKafkaSender sender, IRedisLogService redisLog)
        {
            _logger = logger;
            _redis = redis;
            _sender = sender;
            _config = config;
            _redisLog = redisLog;
        }


        public async Task ConsumeAsync<TKey>(ConsumeResult<TKey, string> consumeResult, CancellationToken cancellationToken = default)
        {
            var dto = JsonConvert.DeserializeObject<ValidationDto>(consumeResult.Message.Value);
            var trxId = dto.Metadata.TrxId;
            var activity = dto.Metadata.Activity;
            var logKey = $"LOG_{activity}_{trxId}";
            var begin = DateTime.UtcNow;
            try
            {
                var data = await _redis.GetAsync<BaseMessageDto>(logKey);

                if (data != null && !data.Status.Equals(EnumStatus.Error))
                {
                    return;
                }

                await _redisLog.LogBegin(dto.Metadata, logKey, begin);

                _logger.LogInformation($"Begin validate with trxId {trxId}");


                /*
                insert logic here
                 */

                await Task.Delay(5_000);

                //result from logic
                var success = true;

                if (!success)
                {
                    var msg = $"reason why it failed";
                    var logfailed = _redisLog.LogFailed(dto.Metadata, logKey, msg);
                    var failed = SendMessage(trxId, msg, EnumStatus.Failed);
                    await Task.WhenAll(logfailed, failed);
                    return;
                }

                _logger.LogInformation($"Done validate with trxId {dto.Metadata.TrxId} in {(DateTime.UtcNow - begin).TotalMilliseconds} milis");

                var logCompleted = _redisLog.LogFinish(dto.Metadata, logKey);
                var completed = SendMessage(trxId, "Validation process completed", EnumStatus.Completed);
                await Task.WhenAll(logCompleted, completed);

            }
            catch (Exception ex)
            {
                var msg = $"Validation process failed because: {ex}";
                var logError = _redisLog.LogError(dto.Metadata, logKey, ex);
                var failed = SendMessage(trxId, msg, EnumStatus.Failed);
                await Task.WhenAll(logError, failed);
                _logger.LogError(ex, "Error occured on trxId : {id} and step : {step} with error message : {ex}", trxId, activity, ex.ToString());
            }
        }

        private async Task SendMessage(Guid trxId, string msg, EnumStatus status)
        {
            var topic = _config.GetValue<string>("Topic:ValidationPartTwoComplete");
            var dto = new BaseMessageDto()
            {
                TrxId = trxId,
                Activity = "PropagateNettingPartTwo",
                Message = msg,
                Status = status,
                Timestamp = DateTime.UtcNow
            };
            await _sender.SendAsync(topic, dto);
        }
    }
}
