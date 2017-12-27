/*
 *  Trade secret of Alibaba Group R&D.
 *  Copyright (c) 2015 Alibaba Group R&D. 
 *
 *  All rights reserved.  This notice is intended as a precaution against
 *  inadvertent publication and does not imply publication or any waiver
 *  of confidentiality.  The year included in the foregoing notice is the
 *  year of creation of the work.
 *
 */

using System;
using System.Collections.Generic;

using PB = Wicture.Aliyun.TableStore.Protocol;
using Model = Aliyun.TableStore.DataModel;
using Aliyun.TableStore.DataModel.ConditionalUpdate;
using Google.Protobuf;

namespace Aliyun.TableStore.Handler
{
    public class ProtoBufferEncoder : PipelineHandler
    {
        private delegate IMessage RequestEncoder(Request.OTSRequest request);
        private Dictionary<string, RequestEncoder> EncoderMap;
        
        public ProtoBufferEncoder(PipelineHandler innerHandler) : base(innerHandler) 
        {
            EncoderMap = new Dictionary<string, RequestEncoder>() {
                { "/CreateTable",          EncodeCreateTable },
                { "/DeleteTable",          EncodeDeleteTable },
                { "/UpdateTable",          EncodeUpdateTable },
                { "/DescribeTable",        EncodeDescribeTable },
                { "/ListTable",            EncodeListTable },
                
                { "/PutRow",               EncodePutRow },
                { "/GetRow",               EncodeGetRow },
                { "/UpdateRow",            EncodeUpdateRow },
                { "/DeleteRow",            EncodeDeleteRow },
                
                { "/BatchWriteRow",        EncodeBatchWriteRow },
                { "/BatchGetRow",          EncodeBatchGetRow },
                { "/GetRange",             EncodeGetRange },
            };
        }
        
        private PB.ColumnType MakeColumnType(Model.ColumnValueType type)
        {
            switch (type) 
            {
                case Model.ColumnValueType.Integer:
                    return PB.ColumnType.Integer;
                case Model.ColumnValueType.String:
                    return PB.ColumnType.String;
                case Model.ColumnValueType.Double:
                    return PB.ColumnType.Double;
                case Model.ColumnValueType.Boolean:
                    return PB.ColumnType.Boolean;
                case Model.ColumnValueType.Binary:
                    return PB.ColumnType.Binary;
                default:
                    throw new OTSClientException(String.Format(
                        "Invalid column value type: {0}", type
                    ));
            }
        }

        private PB.ComparatorType MakeComparatorType(RelationalCondition.CompareOperator oper)
        {
            switch (oper)
            {
                case RelationalCondition.CompareOperator.EQUAL:
                    return PB.ComparatorType.CtEqual;
                case RelationalCondition.CompareOperator.NOT_EQUAL:
                    return PB.ComparatorType.CtNotEqual;
                case RelationalCondition.CompareOperator.GREATER_THAN:
                    return PB.ComparatorType.CtGreaterThan;
                case RelationalCondition.CompareOperator.GREATER_EQUAL:
                    return PB.ComparatorType.CtGreaterEqual;
                case RelationalCondition.CompareOperator.LESS_THAN:
                    return PB.ComparatorType.CtLessThan;
                case RelationalCondition.CompareOperator.LESS_EQUAL:
                    return PB.ComparatorType.CtLessEqual;
                default:
                    throw new OTSClientException(String.Format("Invalid comparator type: {0}", oper));
            }
        }

        private PB.LogicalOperator MakeLogicOperator(CompositeCondition.LogicOperator type)
        {
            switch (type)
            {
                case CompositeCondition.LogicOperator.NOT:
                    return PB.LogicalOperator.LoNot;
                case CompositeCondition.LogicOperator.AND:
                    return PB.LogicalOperator.LoAnd;
                case CompositeCondition.LogicOperator.OR:
                    return PB.LogicalOperator.LoOr;
                default:
                    throw new OTSClientException(String.Format("Invalid logic operator: {0}", type));
            }
        }

        private PB.ColumnConditionType MakeColumnConditionType(ColumnConditionType type)
        {
            switch (type)
            {
                case ColumnConditionType.COMPOSITE_CONDITION:
                    return PB.ColumnConditionType.CctComposite;
                case ColumnConditionType.RELATIONAL_CONDITION:
                    return PB.ColumnConditionType.CctRelation;
                default:
                    throw new OTSClientException(String.Format("Invalid column condition type: {0}", type));
            }
        }
        
        private PB.ColumnSchema MakeColumnSchema(Tuple<string, Model.ColumnValueType> schema)
        {
            return new PB.ColumnSchema { Name = schema.Item1, Type = MakeColumnType(schema.Item2) };
        }
        
        private IEnumerable<PB.ColumnSchema> MakePrimaryKeySchema(DataModel.PrimaryKeySchema schema)
        {
            foreach (var item in schema)
            {
                yield return MakeColumnSchema(item);
            }
        }
        
        private PB.TableMeta MakeTableMeta(Model.TableMeta tableMeta)
        {
            var result = new PB.TableMeta { TableName = tableMeta.TableName };

            foreach (var item in tableMeta.PrimaryKeySchema)
            {
                result.PrimaryKey.Add(MakeColumnSchema(item));
            }

            return result;
        }
        
        private PB.CapacityUnit MakeCapacityUnit(Model.CapacityUnit capacityUnit)
        {
            var result = new PB.CapacityUnit();
            
            if (capacityUnit.Read.HasValue) 
            {
                result.Read = capacityUnit.Read.Value;
            }
            
            if (capacityUnit.Write.HasValue)
            {
                result.Write = capacityUnit.Write.Value;
            }
            return result;
        }

        private PB.ColumnCondition MakeColumnCondition(ColumnCondition cc)
        {
            var result = new PB.ColumnCondition { Type = MakeColumnConditionType(cc.GetType()) };

            if (cc.GetType() == ColumnConditionType.COMPOSITE_CONDITION) {
                result.Condition = BuildCompositeCondition((CompositeCondition)cc);
            } else if (cc.GetType() == ColumnConditionType.RELATIONAL_CONDITION) {
                result.Condition = BuildRelationalCondition((RelationalCondition)cc);
            } else {
                throw new OTSClientException(String.Format("Invalid column condition type: {0}", cc.GetType()));
            }

            return result;
        }

        private ByteString BuildCompositeCondition(CompositeCondition cc)
        {
            var result = new PB.CompositeCondition { Combinator = MakeLogicOperator(cc.Type) };

            foreach (ColumnCondition c in cc.SubConditions)
            {
                result.SubConditions.Add(MakeColumnCondition(c));
            }

            return result.ToByteString();
        }

        private ByteString BuildRelationalCondition(RelationalCondition scc)
        {
            return new PB.RelationCondition
            {
                ColumnName = scc.ColumnName,
                Comparator = MakeComparatorType(scc.Operator),
                ColumnValue = MakeColumnValue(scc.ColumnValue),
                PassIfMissing = scc.PassIfMissing,
            }.ToByteString();
        }
        
        private PB.ReservedThroughput MakeReservedThroughput(Model.CapacityUnit reservedThroughput)
        {
            return new PB.ReservedThroughput { CapacityUnit = MakeCapacityUnit(reservedThroughput) };
        }
        
        private IMessage EncodeCreateTable(Request.OTSRequest request)
        {
            var requestReal = (Request.CreateTableRequest)request;
            return new PB.CreateTableRequest
            {
                TableMeta = MakeTableMeta(requestReal.TableMeta),
                ReservedThroughput = MakeReservedThroughput(requestReal.ReservedThroughput)
            };
        }
        
        private IMessage EncodeDeleteTable(Request.OTSRequest request)
        {
            var requestReal = (Request.DeleteTableRequest)request;
            return new PB.DeleteTableRequest { TableName = requestReal.TableName };
        }

        private IMessage EncodeUpdateTable(Request.OTSRequest request)
        {
            var requestReal = (Request.UpdateTableRequest)request;
            return new PB.UpdateTableRequest
            {
                TableName = requestReal.TableName,
                ReservedThroughput = MakeReservedThroughput(requestReal.ReservedThroughput)
            };
        }

        private IMessage EncodeDescribeTable(Request.OTSRequest request)
        {            
            var requestReal = (Request.DescribeTableRequest)request;
            return new PB.DeleteTableRequest { TableName = requestReal.TableName };
        }

        private IMessage EncodeListTable(Request.OTSRequest request)
        {
            return new PB.ListTableRequest();
        }

        private PB.Condition MakeCondition(Model.Condition condition)
        {
            var result = new PB.Condition();

            if (condition.RowExistenceExpect == Model.RowExistenceExpectation.EXPECT_EXIST) {
                result.RowExistence = PB.RowExistenceExpectation.ExpectExist;
            } else if (condition.RowExistenceExpect == Model.RowExistenceExpectation.EXPECT_NOT_EXIST){
                result.RowExistence = PB.RowExistenceExpectation.ExpectNotExist;
            } else if (condition.RowExistenceExpect == Model.RowExistenceExpectation.IGNORE) {
                result.RowExistence = PB.RowExistenceExpectation.Ignore;
            } else {
                throw new OTSClientException(String.Format("Invalid RowExistenceExpectation: {0}", condition.RowExistenceExpect));
            }

            if (condition.ColumnCondition != null)
            {
                result.ColumnCondition = MakeColumnCondition(condition.ColumnCondition);
            }

            return result;
        }
        
        private PB.ColumnValue MakeColumnValue(DataModel.ColumnValue value)
        {
            var result = new PB.ColumnValue();
            
            if (value == DataModel.ColumnValue.INF_MAX)
            {
                result.Type = PB.ColumnType.InfMax;
            }
            else if (value == DataModel.ColumnValue.INF_MIN)
            {
                result.Type = PB.ColumnType.InfMin;
            }
            else {
                switch (value.Type)
                {
                    case DataModel.ColumnValueType.Binary:
                        result.Type = PB.ColumnType.Binary;
                        result.VBinary = ByteString.CopyFrom(value.BinaryValue);
                        break;
                    case DataModel.ColumnValueType.String:
                        result.Type = PB.ColumnType.String;
                        result.VString = value.StringValue;
                        break;
                        
                    case DataModel.ColumnValueType.Boolean:
                        result.Type = PB.ColumnType.Boolean;
                        result.VBool = value.BooleanValue;
                        break;
                        
                    case DataModel.ColumnValueType.Double:
                        result.Type = PB.ColumnType.Double;
                        result.VDouble = value.DoubleValue;
                        break;
                        
                    case DataModel.ColumnValueType.Integer:
                        result.Type = PB.ColumnType.Integer;
                        result.VInt = value.IntegerValue;
                        break;
                        
                    default:
                        throw new OTSClientException($"Invalid column value type: {value.Type}");
                }
            }
                
            return result;
        }
        
        private PB.Column MakeColumn(string name, DataModel.ColumnValue value)
        {
            return new PB.Column { Name = name, Value = MakeColumnValue(value) };
        }
        
        private IEnumerable<PB.Column> MakeColumns(Dictionary<string, DataModel.ColumnValue> columns)
        {
            foreach (var column in columns)
            {
                yield return MakeColumn(column.Key, column.Value);
            }
        }
        
        private IMessage EncodePutRow(Request.OTSRequest request)
        {
            var requestReal = (Request.PutRowRequest)request;
            var result = new PB.PutRowRequest
            {
                TableName = requestReal.TableName,
                Condition = MakeCondition(requestReal.Condition)
            };

            result.PrimaryKey.AddRange(MakeColumns(requestReal.PrimaryKey));
            result.AttributeColumns.AddRange(MakeColumns(requestReal.Attribute));

            return result;
        }
        
        private IMessage EncodeGetRow(Request.OTSRequest request)
        {            
            var requestReal = (Request.GetRowRequest)request;

            var result = new PB.GetRowRequest { TableName = requestReal.QueryCriteria.TableName };
            result.PrimaryKey.AddRange(MakeColumns(requestReal.QueryCriteria.RowPrimaryKey));
            result.ColumnsToGet.AddRange(requestReal.QueryCriteria.GetColumnsToGet());

            if (requestReal.QueryCriteria.Filter != null)
            {
                result.Filter = MakeColumnCondition(requestReal.QueryCriteria.Filter);
            }

            return result;
        }
        
        private PB.ColumnUpdate MakeColumnUpdateForDelete(string columnName)
        {
            return new PB.ColumnUpdate { Name = columnName, Type = PB.OperationType.Delete };
        }
        
        private PB.ColumnUpdate MakeColumnUpdateForPut(string columnName, DataModel.ColumnValue value)
        {
            return new PB.ColumnUpdate { Name = columnName, Type = PB.OperationType.Put, Value = MakeColumnValue(value) };
        }

        private IEnumerable<PB.ColumnUpdate> MakeUpdateOfAttribute(DataModel.UpdateOfAttribute update)
        {
            foreach (var item in update.AttributeColumnsToPut)
            {
                yield return MakeColumnUpdateForPut(item.Key, item.Value);
            }
            
            foreach (var item in update.AttributeColumnsToDelete)
            {
                yield return MakeColumnUpdateForDelete(item);
            }
        }
        
        private IMessage EncodeUpdateRow(Request.OTSRequest request)
        {
            var requestReal = (Request.UpdateRowRequest)request;
            var result = new PB.UpdateRowRequest
            {
                TableName = requestReal.TableName,
                Condition = MakeCondition(requestReal.Condition)
            };

            result.PrimaryKey.AddRange(MakeColumns(requestReal.PrimaryKey));
            result.AttributeColumns.AddRange(MakeUpdateOfAttribute(requestReal.UpdateOfAttribute));

            return result;
        }
        
        private IMessage EncodeDeleteRow(Request.OTSRequest request)
        {
            var requestReal = (Request.DeleteRowRequest)request;

            var result = new PB.DeleteRowRequest
            {
                TableName = requestReal.TableName,
                Condition = MakeCondition(requestReal.Condition)
            };

            result.PrimaryKey.AddRange(MakeColumns(requestReal.PrimaryKey));

            return result;
        }
        
        private PB.TableInBatchWriteRowRequest 
            MakeTableInBatchWriteRowRequest(string tableName, DataModel.RowChanges rowChanges)
        {
            var result = new PB.TableInBatchWriteRowRequest { TableName = tableName };
            
            foreach (var op in rowChanges.PutOperations)
            {
                var rowRequest = new PB.PutRowInBatchWriteRowRequest { Condition = MakeCondition(op.Item1) };
                rowRequest.PrimaryKey.AddRange(MakeColumns(op.Item2));
                rowRequest.AttributeColumns.AddRange(MakeColumns(op.Item3));
                result.PutRows.Add(rowRequest);
            }

            foreach (var op in rowChanges.UpdateOperations)
            {
                var rowRequest = new PB.UpdateRowInBatchWriteRowRequest { Condition = MakeCondition(op.Item1) };
                rowRequest.PrimaryKey.AddRange(MakeColumns(op.Item2));
                rowRequest.AttributeColumns.AddRange(MakeUpdateOfAttribute(op.Item3));
                result.UpdateRows.Add(rowRequest);
            }
            
            foreach (var op in rowChanges.DeleteOperations)
            {
                var rowRequest = new PB.DeleteRowInBatchWriteRowRequest { Condition = MakeCondition(op.Item1) };
                rowRequest.PrimaryKey.AddRange(MakeColumns(op.Item2));
                result.DeleteRows.Add(rowRequest);                
            }
            
            return result;
        }
        
        private IMessage EncodeBatchWriteRow(Request.OTSRequest request)
        {
            var requestReal = (Request.BatchWriteRowRequest)request;
            var result = new PB.BatchWriteRowRequest();
            
            foreach (var item in requestReal.RowChangesGroupByTable) {
                result.Tables.Add(MakeTableInBatchWriteRowRequest(item.Key, item.Value));
            }
            
            return result;
        }
        
        private PB.TableInBatchGetRowRequest 
            MakeTableInBatchGetRowRequest(Model.MultiRowQueryCriteria criteria)
        {
            var result = new PB.TableInBatchGetRowRequest { TableName = criteria.TableName };
            
            foreach (var primaryKey in criteria.GetRowKeys())
            {
                var request = new PB.RowInBatchGetRowRequest();
                request.PrimaryKey.AddRange(MakeColumns(primaryKey));
                result.Rows.Add(request);
            }
            
            if (criteria.GetColumnsToGet() != null) {
                result.ColumnsToGet.AddRange(criteria.GetColumnsToGet());
            }

            if (criteria.Filter != null)
            {
                result.Filter = MakeColumnCondition(criteria.Filter);
            }
            return result;
        }
        
        private IMessage EncodeBatchGetRow(Request.OTSRequest request)
        {
            var requestReal = (Request.BatchGetRowRequest)request;
            var result = new PB.BatchGetRowRequest();
            
            foreach (var criterias in requestReal.GetCriterias())
            {
                result.Tables.Add(MakeTableInBatchGetRowRequest(criterias));
            }
            
            return result;
        }
        
        private PB.Direction MakeDirection(Request.GetRangeDirection direction)
        {
            switch (direction)
            {
                case Request.GetRangeDirection.Forward:
                    return PB.Direction.Forward;
                case Request.GetRangeDirection.Backward:
                    return PB.Direction.Backward;
                    
                default:
                    throw new OTSClientException($"Invalid direction: {direction}");
            }
        }
        
        private IMessage EncodeGetRange(Request.OTSRequest request)
        {
            var requestReal = (Request.GetRangeRequest)request;
            var result = new PB.GetRangeRequest()
            {
                TableName = requestReal.QueryCriteria.TableName,
                Direction = MakeDirection(requestReal.QueryCriteria.Direction)
            };
            
            if (requestReal.QueryCriteria.GetColumnsToGet() != null) {
                result.ColumnsToGet.AddRange(requestReal.QueryCriteria.GetColumnsToGet());
            }
            
            if (requestReal.QueryCriteria.Limit != null) 
            {
                result.Limit = (int)requestReal.QueryCriteria.Limit;
            }

            if (requestReal.QueryCriteria.Filter != null)
            {
                result.Filter = MakeColumnCondition(requestReal.QueryCriteria.Filter);
            }
            
            result.InclusiveStartPrimaryKey.AddRange(MakeColumns(requestReal.QueryCriteria.InclusiveStartPrimaryKey));
            result.ExclusiveEndPrimaryKey.AddRange(MakeColumns(requestReal.QueryCriteria.ExclusiveEndPrimaryKey));
            
            return result;
        }
        
        private void LogEncodedMessage(Context context, IMessage message)
        {
            if (context.ClientConfig.OTSDebugLogHandler != null) {
                var msgString = $"OTS Request API: {context.APIName} Protobuf: {message}\n";
                context.ClientConfig.OTSDebugLogHandler(msgString);
            }
        }

        public override void HandleBefore(Context context)
        {
            var encoder = EncoderMap[context.APIName];
            var message = encoder(context.OTSRequest);
            LogEncodedMessage(context, message);
            context.HttpRequestBody = message.ToByteArray();
            InnerHandler.HandleBefore(context);
        }

        public override void HandleAfter(Context context) 
        {
            InnerHandler.HandleAfter(context);
        }
    }
}
