using SampleCalculation.BLL.Enum;


namespace SampleCalculation.BLL
{
    public delegate IConsumeProcess ServiceResolver(EnumServiceType serviceType);
}
