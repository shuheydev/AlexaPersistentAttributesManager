using System;
using System.Collections.Generic;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DocumentModel;
using Amazon.DynamoDBv2.Model;

namespace Alexa.PersistentAttributes.DynamoDB
{
    /// <summary>
    /// 
    /// </summary>
    public class AttributesManager
    {
        private IAmazonDynamoDB Client { get; }
        public Table Table { get; private set; }
        public Document Attributes { get; private set; }
        public string UserId { get; }
        public string TableName { get; }

        public AttributesManager(string userId, string tableName)
        {
            this.Client = new AmazonDynamoDBClient();
            this.TableName = tableName;
            this.UserId = userId;
            this.Attributes = new Document();

            try
            {
                CreateOrConnectTable();
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
                throw;
            }

        }





        /// <summary>
        /// 同名のテーブルが存在するかをチェックします。
        /// </summary>
        /// <param name="tableName"></param>
        /// <returns></returns>
        private bool IsTableExist(string tableName)
        {
            //テーブル一覧を取得
            var tableList = Client.ListTablesAsync().Result;
            //TableNamesプロパティをチェック
            return tableList.TableNames.Exists(s => s.Equals(tableName));
        }

        public void SetPersistentAttributes(string attrName, DynamoDBEntry value)
        {
            Attributes[attrName] = value;
        }

        /// <summary>
        /// データをテーブルに追加します。
        /// </summary>
        public void SavePersistentAttributes()
        {
            var item = new Document
            {
                ["id"] = UserId,
                ["attributes"] = Attributes,
            };

            var result = Table.PutItemAsync(item).Result;//Wait()じゃだめ？
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public Document GetPersistentAttributes()
        {
            var result = Table.GetItemAsync(UserId).Result;

            var attributes = result?["attributes"].AsDocument();

            return attributes;
        }


        /// <summary>
        /// Alexaスキル用のテーブルを作成。
        /// </summary>
        private void CreateOrConnectTable()
        {
            //テーブル存在チェック
            if (!this.IsTableExist(this.TableName))
            {
                //テーブル作成リクエストの作成
                var request = new CreateTableRequest
                {
                    TableName = this.TableName,
                    AttributeDefinitions = new List<AttributeDefinition>()
                    {
                        new AttributeDefinition
                        {
                            AttributeName = "id",
                            AttributeType = "S"
                        }
                    },
                    KeySchema = new List<KeySchemaElement>
                    {
                        new KeySchemaElement
                        {
                            AttributeName = "id",
                            KeyType = KeyType.HASH //Partition key
                        },
                    },
                    ProvisionedThroughput = new ProvisionedThroughput
                    {
                        ReadCapacityUnits = 5,
                        WriteCapacityUnits = 5
                    }
                };

                //テーブル作成
                Client.CreateTableAsync(request).Wait();

                //テーブルの使用準備ができるまで待つ必要がある。
                //テーブル作成リクエストを送って、処理が戻ってきても、テーブルが利用可能になるまで数秒かかるのね(´・ω・｀)
                WaitUntilTableReady();
            }



            //テーブル接続
            this.ConnectTable();
        }

        /// <summary>
        /// テーブルが利用可能になるまで待つ。
        /// </summary>
        private void WaitUntilTableReady()
        {
            string status = null;

            do
            {
                System.Threading.Thread.Sleep(1000); //3000ミリ秒待つ

                try
                {
                    var res = this.Client.DescribeTableAsync(new DescribeTableRequest
                    {
                        TableName = this.TableName
                    }).Result;

                    status = res.Table.TableStatus;
                }
                catch (ResourceNotFoundException e)
                {
                    Console.WriteLine(e);
                    throw;
                }

            } while (status!=TableStatus.ACTIVE);
        }

        /// <summary>
        /// テーブルに接続する。
        /// </summary>
        /// <returns></returns>
        private bool ConnectTable()
        {
            bool result = true;

            try
            {
                this.Table = Table.LoadTable(Client, this.TableName);
            }
            catch
            {
                result = false;
            }

            return result;
        }


    }
}
