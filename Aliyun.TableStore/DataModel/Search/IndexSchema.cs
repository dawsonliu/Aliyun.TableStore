using System.Collections.Generic;
using Aliyun.TableStore.DataModel.Search.Sort;

namespace Aliyun.TableStore.DataModel.Search
{
    public class IndexSchema
    {
        public IndexSchema() { }

        public IndexSetting IndexSetting { get; set; }

        public List<FieldSchema> FieldSchemas { get; set; }

        /// <summary>
        /// 自定义索引的预排序方式
        /// </summary>
        public Sort.Sort IndexSort { get; set; }
    }
}
