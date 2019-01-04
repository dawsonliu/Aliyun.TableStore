﻿using Aliyun.TableStore.DataModel;

namespace Aliyun.TableStore.Request
{
    public class CreateGlobalIndexRequest : OTSRequest
    {
        public string MainTableName { get; set; }

        public IndexMeta IndexMeta { get; set; }

        public bool IncludeBaseData { get; set; }

        public CreateGlobalIndexRequest(string mainTableName, IndexMeta indexMeta)
        {
            this.MainTableName = mainTableName;
            this.IndexMeta = indexMeta;
        }
    }
}
