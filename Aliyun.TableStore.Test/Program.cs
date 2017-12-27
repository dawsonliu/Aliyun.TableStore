using Aliyun.TableStore.DataModel;
using Aliyun.TableStore.Request;
using Aliyun.TableStore.Response;
using System;
using System.Collections.Generic;
using System.Diagnostics;

namespace Aliyun.TableStore.Test
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Aliyun Table Store SDK for .NET Samples!");

            try
            {
                while (Console.ReadLine() != "A")
                {
                    GetRow();
                }
            }
            catch (OTSClientException ex)
            {
                Console.WriteLine("Failed with client exception:{0}", ex.Message);
            }
            catch (OTSServerException ex)
            {
                Console.WriteLine("Failed with server exception:{0}, {1}", ex.Message, ex.RequestID);
            }
            catch (Exception ex)
            {
                Console.WriteLine("Failed with error info: {0}", ex.Message);
            }

            Console.WriteLine("Press any key to continue . . . ");
            Console.ReadKey(true);
        }

        public static void GetRow()
        {
            Console.WriteLine("Start get row...");
            // PrepareTable();
            OTSClient otsClient = Config.GetClient();

            // 定义行的主键，必须与创建表时的TableMeta中定义的一致
            PrimaryKey primaryKey = new PrimaryKey();
            primaryKey.Add("key", new ColumnValue("the key"));

            Stopwatch stopwatch = Stopwatch.StartNew();
            GetRowRequest request = new GetRowRequest("tableName", primaryKey); // 未指定读哪列，默认读整行
            GetRowResponse response = otsClient.GetRow(request);
            stopwatch.Stop();

            PrimaryKey primaryKeyRead = response.PrimaryKey;
            AttributeColumns attributesRead = response.Attribute;

            Console.WriteLine("Primary key read: ");
            foreach (KeyValuePair<string, ColumnValue> entry in primaryKeyRead)
            {
                Console.WriteLine(entry.Key + ":" + PrintColumnValue(entry.Value));
            }

            Console.WriteLine("Attributes read: ");
            foreach (KeyValuePair<string, ColumnValue> entry in attributesRead)
            {
                Console.WriteLine(entry.Key + ":" + PrintColumnValue(entry.Value));
            }

            Console.WriteLine("Get row succeed.");
            Console.WriteLine(stopwatch.Elapsed);
        }


        private static string PrintColumnValue(ColumnValue value)
        {
            switch (value.Type)
            {
                case ColumnValueType.String: return value.StringValue;
                case ColumnValueType.Integer: return value.IntegerValue.ToString();
                case ColumnValueType.Boolean: return value.BooleanValue.ToString();
                case ColumnValueType.Double: return value.DoubleValue.ToString();
                case ColumnValueType.Binary: return value.BinaryValue.ToString();
            }

            throw new Exception("Unknow type.");
        }
    }
}
