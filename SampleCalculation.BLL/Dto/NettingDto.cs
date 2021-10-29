using System;

namespace SampleCalculation.BLL.Dto
{
    public class NettingDto
    {
        public string InvestorId { get; set; }
        public string ContractId { get; set; }
        public string TradeFeedId { get; set; }
        public int Price { get; set; }
        public DateTime Timestamp { get; set; }
        public BaseMessageDto Metadata { get; set; }
    }
}
