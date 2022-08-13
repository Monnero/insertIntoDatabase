using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Text;

namespace insertOpeka
{

    class Program : OpekaParams
    {
        static string tableName = "opeka1";
        static int g = 0;
        static int i = 1;
        //const string connectionString = @"Data Source=WS10100040012;Initial Catalog=ehd;Integrated Security=False;User Id=bot;Password=qwerty1";
        static void Main(string[] args)
        {
            Count(ref g);
            Console.WriteLine("Количество до: " + g);

            TruncateOpeka();
            DirectoryInfo dir = new DirectoryInfo(@"D:\ОЛСП\Сведения ЗАГС\utilita1\");
            foreach (var file in dir.GetFiles())
            {
                OpekaParams opekaString = new OpekaParams();
                opekaString.InsertOpeka(file.ToString());
                
                //port1(item.Name);
                i++;

            }

            Count(ref g);
            Console.WriteLine("Количество после: " + g);
            Console.ReadKey();

        }

        //static public void port(string namef)
        // {
        //     string sqlExpression5 = @"BULK INSERT zags1 FROM '\\10.101.13.10\Tatarstan\ОЛСП\Сведения ЗАГС\utilita\" + namef + @"' WITH (   CODEPAGE = 'ACP',     FIRSTROW = 1 ,     FIELDTERMINATOR = ';' ,      ROWTERMINATOR = '\n' ,      TABLOCK );";
        //         string sqlExpression6 = @"WITH CTE AS(  SELECT id1,id2 ,id3 ,id4 ,id5 ,id6 ,id7 ,id8 ,id9 ,id10,id11,id12,id13,id14,id15,id16,id17,id18,id19,id20,id21,id22,id23,id24,id25,id26,id27,id28,id29,id30,id31,id32,id33,id34,id35,id36,id37,id38,id39,id40,id41,id42,id43,id44,id45,id46,id47,id48,id49,id50,id51,id52,  RN = ROW_NUMBER()OVER(PARTITION BY id1 ORDER BY id1)  FROM zags1)     DELETE FROM CTE WHERE RN > 1";

        //         using (SqlConnection connection5 = new SqlConnection(connectionString))
        //         {
        //             connection5.Open();
        //             SqlCommand command5 = new SqlCommand(sqlExpression5, connection5);
        //             command5.ExecuteNonQuery();

        //         }

        //         using (SqlConnection connection5 = new SqlConnection(connectionString))
        //         {
        //             connection5.Open();
        //             SqlCommand command5 = new SqlCommand(sqlExpression6, connection5);
        //             command5.ExecuteNonQuery();

        //         }

        // }

        //static public void port1(string namef) //заполнение таблицы
        //{
        //    string sqlExpression5 = @"BULK INSERT opeka FROM 'D:\ОЛСП\Сведения ЗАГС\utilitaTestTest\" + namef + @"' WITH (   CODEPAGE = 'ACP',     FIRSTROW = 2 ,     FIELDTERMINATOR = ';' ,      ROWTERMINATOR = '\n' ,      TABLOCK );";

        //    using (SqlConnection connection5 = new SqlConnection(connectionString))
        //    {

        //        Console.WriteLine("Начало загрузки " + i);
        //        connection5.Open();
        //        SqlCommand command5 = new SqlCommand(sqlExpression5, connection5);
        //        command5.CommandTimeout = 5000;
        //        command5.ExecuteNonQuery();
        //        Console.WriteLine("Конец загрузки " + i);
        //    }
        //}

        static public void TruncateOpeka() // очищение таблицы
        {

            string sqlExpression6 = $"truncate table {tableName}";

            using (SqlConnection connection5 = new SqlConnection(OpekaParams.connectionString))
            {

                connection5.Open();
                Console.WriteLine("Начало очистки");
                SqlCommand command5 = new SqlCommand(sqlExpression6, connection5);
                command5.CommandTimeout = 3000;
                command5.ExecuteNonQuery();
                Console.WriteLine("Конец очистки");
            }
        }

        static public void Count(ref int g)
        {
            string sqlExpression5 = $"SELECT COUNT(id1) as f  FROM [ehd].[dbo].[{tableName}]";

            using (SqlConnection connection5 = new SqlConnection(connectionString))
            {
                connection5.Open();
                SqlCommand command5 = new SqlCommand(sqlExpression5, connection5);
                SqlDataReader reader = command5.ExecuteReader();
                while (reader.Read())
                {
                    g = Convert.ToInt32(reader["f"].ToString());
                }
            }
        }
        //public static void InsertFile(string fileName)
        //{
        //    Console.WriteLine($"{fileName} в функции!");
        //    using (SqlConnection connection = new SqlConnection(connectionString))
        //    {
        //        connection.Open();

        //        var lines = File.ReadAllLines(@"D:\ОЛСП\Сведения ЗАГС\utilita1/" + fileName + @"", Encoding.Default);
        //        string[][] text = new string[lines.Length][];

        //        for (var i = 0; i < text.Length; i++)
        //        {
        //            text[i] = lines[i].Split(';');
        //        }

        //        for (int t = 0; t < text.Length; t++)
        //        {
        //            string expressionInseropeka1 = "INSERT INTO opeka1 VALUES ('" + text[t][0] + "','" + text[t][1] + "','" + text[t][2].Replace("(null)", "null") + "','" + text[t][3] + "','" + text[t][4] + "','" + text[t][5] + "','" + text[t][6] + "','" + text[t][7] + "','" + text[t][8] + "','" + text[t][9] + "','" + text[t][10] + "')";
        //            OpekaParams opekaString = new OpekaParams();
        //            opekaString.ExecuteExpression(expressionInserOpeka);
        //        }
        //    }
        //}
    }
}
