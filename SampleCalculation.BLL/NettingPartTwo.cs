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
    public class NettingPartTwo : IConsumeProcess
    {
        private readonly ILogger _logger;
        private readonly IRedisService _redis;
        private readonly IKafkaSender _sender;
        private readonly IConfiguration _config;
        private readonly IRedisLogService _redisLog;

        public NettingPartTwo(ILogger<NettingPartTwo> logger, IConfiguration config, IRedisService redis, IKafkaSender sender, IRedisLogService redisLog)
        {
            _logger = logger;
            _redis = redis;
            _sender = sender;
            _config = config;
            _redisLog = redisLog;
        }


        public async Task ConsumeAsync<TKey>(ConsumeResult<TKey, string> consumeResult, CancellationToken cancellationToken = default)
        {
            var incomingPositionData = JsonConvert.DeserializeObject<NettingDto>(consumeResult.Message.Value);
            var trxId = incomingPositionData.Metadata.TrxId;
            var activity = incomingPositionData.Metadata.Activity;

            var todayDate = DateTime.UtcNow.ToString("yyyy-MM-dd");
            var yesterdayDate = DateTime.UtcNow.AddDays(-1).ToString("yyyy-MM-dd");

            var dataKey = incomingPositionData.ContractId + incomingPositionData.InvestorId;

            var logKey = $"LOG_{activity}_{trxId}_{incomingPositionData.InvestorId}_{incomingPositionData.ContractId}";
            var dataCurrentPositionKey = $"DATA_{todayDate}_{incomingPositionData.InvestorId}_{incomingPositionData.ContractId}";
            var dataOpenPositionKey = $"DATA_OPEN_{yesterdayDate}_{incomingPositionData.InvestorId}_{incomingPositionData.ContractId}";

            var failedKey = $"FAILED_{activity}_{trxId}";
            var completedKey = $"COMPLETED_{activity}_{trxId}";
            try
            {
                var begin = DateTime.UtcNow;

                var logTrx = await _redis.GetAsync<BaseMessageDto>(logKey); //checking apakah proses sudah mulai oleh instance lain

                if (logTrx != null && !logTrx.Status.Equals(EnumStatus.Error))
                {
                    return;
                }

                await _redisLog.LogBegin(incomingPositionData.Metadata, logKey, begin);//log ke redis untuk menandakan proses akan dimulai

                _logger.LogDebug($"Validating investorId :{incomingPositionData.InvestorId} contractId:{incomingPositionData.ContractId}");


                // ini cuma dummy logic 

                //get data state from previous step, handle it if still null
                var currentPosData = await _redis.GetAsync<NettingDto>(dataCurrentPositionKey);
                if (currentPosData == null)
                {
                    currentPosData = incomingPositionData;
                }

                //get data open position, handle it if null
                var openPosData = await _redis.GetAsync<NettingDto>(dataOpenPositionKey);
                if (openPosData == null)
                {
                    openPosData = incomingPositionData;
                }


                /*
                   insert calulation logic here
                */

                await Task.Delay(5_000);

                //update data state
                currentPosData.Price += incomingPositionData.Price;

                //update data open position state
                openPosData.Price += incomingPositionData.Price - 1;

                var success = true;

                if (!success)
                {
                    var msg = $"reason why it failed";
                    var logfailed = _redisLog.LogFailed(incomingPositionData.Metadata, logKey, msg);
                    var failed = SendMessage(incomingPositionData.Metadata, EnumStatus.Failed);
                    var registerFailedTrxId = _redis.AddToSetAsync(failedKey, dataKey);
                    await Task.WhenAll(logfailed, failed, registerFailedTrxId);
                    return;
                }

                _logger.LogInformation($"Done netting with trxId {trxId} in {(DateTime.UtcNow - begin).TotalMilliseconds} milis");

                //save to db if necesary

                var saveState = _redis.SaveAsync(dataCurrentPositionKey, currentPosData);
                var saveStatePrev = _redis.SaveAsync(dataOpenPositionKey, openPosData);

                var registerCompletedTrxId = _redis.AddToSetAsync(completedKey, dataKey);//nandain kalau proses saat ini(investor,kontrak) berhasil

                var logFinish = _redisLog.LogFinish(incomingPositionData.Metadata, logKey);//log ke redis proses saat ini selesai

                var complete = SendMessage(incomingPositionData.Metadata, EnumStatus.Completed);//send message untuk nandain proses selesai

                await Task.WhenAll(saveState, saveStatePrev, registerCompletedTrxId, logFinish, complete);
            }
            catch (Exception ex)
            {
                var logError = _redisLog.LogError(incomingPositionData.Metadata, logKey, ex);
                var failed = SendMessage(incomingPositionData.Metadata, EnumStatus.Error);
                var registerFailedTrxId = _redis.AddToSetAsync(failedKey, dataKey);
                await Task.WhenAll(logError, failed, registerFailedTrxId);
                _logger.LogError(ex, "Error occured on trxId : {id} and step : {step}, and data: {data} with error message : {ex}", trxId, activity, dataKey, ex.ToString());
            }
        }

        private async Task SendMessage(BaseMessageDto dto, EnumStatus status)
        {
            var topic = _config.GetValue<string>("Topic:NettingPartTwoStatus");
            var msg = new BaseMessageDto()
            {
                TrxId = dto.TrxId,
                Activity = dto.Activity,
                Timestamp = DateTime.UtcNow,
                Status = status
            };
            await _sender.SendAsync(topic, msg);
        }
    }
}
