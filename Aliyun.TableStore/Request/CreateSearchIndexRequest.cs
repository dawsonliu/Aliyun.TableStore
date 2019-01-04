using Aliyun.TableStore.DataModel.Search;

namespace Aliyun.TableStore.Request
{
    public class CreateSearchIndexRequest : OTSRequest
    {
        public string TableName { get; set; }
        public string IndexName { get; set; }
        public IndexSchema IndexSchame { get; set; }

        public CreateSearchIndexRequest(string tableName, string indexName)
        {
            this.TableName = tableName;
            this.IndexName = indexName;
        }
    }
}
