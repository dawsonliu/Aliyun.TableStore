namespace Aliyun.TableStore.Test
{
    internal class Config
    {
        public static string AccessKeyId = "<Your Access Key Id>";

        public static string AccessKeySecret = "<Your Access Key Secret>";

        public static string Endpoint = "https://name.cn-shanghai.ots.aliyuncs.com";

        public static string InstanceName = "name";

        private static OTSClient OtsClient = null;

        public static OTSClient GetClient()
        {
            if (OtsClient != null)
            {
                return OtsClient;
            }

            OTSClientConfig config = new OTSClientConfig(Endpoint, AccessKeyId, AccessKeySecret, InstanceName);
            config.OTSDebugLogHandler = null;
            config.OTSErrorLogHandler = null;
            OtsClient = new OTSClient(config);
            return OtsClient;
        }
    }
}
