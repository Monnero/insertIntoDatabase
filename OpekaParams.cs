using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Text;
using System.Configuration;
namespace insertOpeka
{
    class OpekaParams
    {
        static readonly string tableName = "opeka1";
        internal static string connectionString = ConfigurationManager.ConnectionStrings["DefaultConnection"].ConnectionString;
        private string param2;
        private string param4;
        private string param10;

        public string Param1 { get; set; }
        public string Param2 { get => param2; set => param2 = value.Replace("(null)", "null"); }
        public string Param3 { get; set; }
        public string Param4 { get => param4; set => param4 = value.Replace("(null)", "null"); }
        public string Param5 { get; set; }
        public string Param6 { get; set; }
        public string Param7 { get; set; }
        public string Param8 { get; set; }
        public string Param9 { get; set; }
        public string Param10 { get => param10; set => param10 = value.Replace("'", null); }
        public string Param11 { get; set; }
        public void InsertOpeka(string fileName)
        {
            Console.WriteLine($"Читаем {fileName}");
            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                var file = File.ReadAllLines(@"D:\ОЛСП\Сведения ЗАГС\utilita1/" + fileName + @"", Encoding.Default);
                List<OpekaParams> list = file.Select(s => s.Split(';'))
                                    .Select(opeka => new OpekaParams
                                    {
                                        Param1 = opeka[0],
                                        Param2 = opeka[1],
                                        Param3 = opeka[2],
                                        Param4 = opeka[3],
                                        Param5 = opeka[4],
                                        Param6 = opeka[5],
                                        Param7 = opeka[6],
                                        Param8 = opeka[7],
                                        Param9 = opeka[8],
                                        Param10 = opeka[9],
                                        Param11 = opeka[10]
                                    }).ToList();
                connection.Open();
                
                int insertPack = 100;
                bool b = true;
                StringBuilder stringBuilder = new StringBuilder();
                for (int i = 1; i < list.Count(); i += insertPack)
                {
                    
                    if (i + insertPack <= list.Count() && b)
                    {

                        InserIntoTable(stringBuilder, insertPack, list, i);
                    }
                    else
                    {
                        b = false;
                        insertPack = list.Count() - i;
                        InserIntoTable(stringBuilder, insertPack, list, i);

                    }
                }
            }
        }
        public void ExecuteExpression(string str)
        {
            string sqlExpression5 = str;
            using (SqlConnection connection5 = new SqlConnection(connectionString))
            {
                connection5.Open();
                SqlCommand command5 = new SqlCommand(sqlExpression5, connection5);
                command5.ExecuteNonQuery();
            }
        }
        public void InserIntoTable(StringBuilder stringBuilder, int insertPack, List<OpekaParams> list, int i)
        {
            stringBuilder.Append($"INSERT INTO {tableName} VALUES ");
            for (int j = 0; j < insertPack; j++)
            {
                stringBuilder.Append("(" + list[i + j].Param1
                                              + ","
                                              + list[i + j].Param2
                                              + ",'"
                                              + list[i + j].Param3
                                              + "','"
                                              + list[i + j].Param4
                                              + "','"
                                              + list[i + j].Param5
                                              + "','"
                                              + list[i + j].Param6
                                              + "','"
                                              + list[i + j].Param7
                                              + "','"
                                              + list[i + j].Param8
                                              + "','"
                                              + list[i + j].Param9
                                              + "','"
                                              + list[i + j].Param10
                                              + "','"
                                              + list[i + j].Param11
                                              + "'),");
            }
            stringBuilder.Length -= 1;
            ExecuteExpression(stringBuilder.ToString());
            stringBuilder.Length = 0;
        }
    }
}
