using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using SampleCalculation.BLL;
using SampleCalculation.BLL.Kafka;
using SampleCalculation.BLL.Redis;
using SampleCalculation.External;
using SampleCalculation.Redis.BLL;

namespace SampleCalculation.FrontEndApi
{
    public class Startup
    {
        public Startup(IConfiguration configuration)
        {
            Configuration = configuration;
        }

        public IConfiguration Configuration { get; }

        // This method gets called by the runtime. Use this method to add services to the container.
        public void ConfigureServices(IServiceCollection services)
        {
            services.AddSingleton<IHostedService, ConsumerService>();
            services.AddSingleton<IRedisService, RedisService>();
            services.AddSingleton<IKafkaSender, KafkaSender>();
            services.AddSingleton<IRedisLogService, RedisLogService>();
            services.AddTransient<ValidationPartTwo>();
            services.AddTransient<NettingPartTwo>();
        }

        // This method gets called by the runtime. Use this method to configure the HTTP request pipeline.
        public void Configure(IApplicationBuilder app, IWebHostEnvironment env)
        {
            if (env.IsDevelopment())
            {
                app.UseDeveloperExceptionPage();
            }
        }
    }
}
