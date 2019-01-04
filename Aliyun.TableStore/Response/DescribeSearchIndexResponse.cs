using Aliyun.TableStore.DataModel.Search;

namespace Aliyun.TableStore.Response
{
    public class DescribeSearchIndexResponse : OTSResponse
    {
        public IndexSchema Schema { get; set; }
        public SyncStat SyncStat { get; set; }

    }
}
