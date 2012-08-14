using System;
using System.Collections.Generic;
using System.Collections;
using System.Linq;
using System.Text;
using MySql.Data.MySqlClient;

namespace corpus_de_duplicates
{
    class Table
    {
        #region
        /// <summary>
        /// mysql's server_config format:
        /// ipaddr: ***.***.***.***
        /// userid: ****
        /// userpassword: ******
        /// database: *****
        /// </summary>
        private Dictionary<string, string> _server_config;
        /// <summary>
        /// bits count of simhash's fingerprints
        /// ps: now I prefer 64bits
        /// </summary>
        private int _count_bits;
        /// <summary>
        /// the count fingerprints be splited
        /// </summary>
        private int _count_blocks;
        /// <summary>
        /// the detect_duplicates limit based on Haming Distance
        /// </summary>
        private int _num_diff_bits;
        /// <summary>
        /// bit_mask
        /// </summary>
        private List<ulong> _bit_mask;
        /// <summary>
        /// the bit_mask's combination based on 
        /// </summary>
        private Dictionary<string, ulong> _combine_bit_mask;

        private combinations<int> _combinations;
        #endregion

        #region
        public Table(Dictionary<string, string> server_config, int count_bits, int count_blocks, int num_diff_bits, List<ulong> bit_mask)
        {
            _server_config = new Dictionary<string,string>(server_config);
            _count_bits = count_bits;
            _count_blocks = count_blocks;
            _num_diff_bits = num_diff_bits;
            _bit_mask = new List<ulong>(bit_mask);
            generate_combine();
        }
        public void reset()
        {
            MySqlConnection conn = new MySqlConnection();
            if (connect(_server_config, ref conn))
            {
                List<string> sql_cols = new List<string>();
                List<string> sql_indexes = new List<string>();
                foreach (IList<int> item in _combinations)
                {
                    string com = getstring<int>(item);
                    sql_cols.Add(string.Format("{0} BIGINT UNSIGNED NOT NULL", "combine_" + com));
                    sql_indexes.Add(string.Format("CREATE INDEX {0} ON corpus({1}); ", "index_"+com, "combine_"+com));
                }

                string sql = "DROP TABLE IF EXISTS `corpus`; " +
                    "CREATE TABLE corpus( " +
                    "id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, " +
                    "article TEXT CHARACTER SET utf8 NOT NULL, " +
                    "fingerprints BIGINT UNSIGNED NOT NULL, " +
                    string.Join(", ", sql_cols.ToArray())+ ");" +
                    string.Join("", sql_indexes.ToArray());
                MySqlCommand cmd = new MySqlCommand(sql, conn);
                try
                {
                    cmd.ExecuteNonQuery();
                }
                catch (MySqlException ex)
                {
                    Console.WriteLine("ERROR occured at create tables, MESSAGE:" + ex);
                }
                finally
                {
                    conn.Close();
                }
            }
        }

        private void generate_combine()
        {
            _combine_bit_mask = new Dictionary<string, ulong>();

            IList<int> array = Enumerable.Range(0, _count_blocks).ToList<int>();
            _combinations = new combinations<int>(array, _count_blocks - _num_diff_bits);
            foreach (IList<int> item in _combinations)
            {
                string com = getstring<int>(item);

                ulong combine_mask = 0;
                foreach (int li in item)
	            {
                    combine_mask^=_bit_mask[li];
                }
                _combine_bit_mask.Add(com, combine_mask);
            }
        }

        private static string getstring<T>(IList<T> list)
        {
            StringBuilder sb = new StringBuilder();
            foreach (T item in list)
            {
                sb.Append(item.ToString());
            }
            return sb.ToString();
        }

        private bool connect(Dictionary<string, string> server_config, ref MySqlConnection conn)
        {
            if (conn != null)
                conn.Close();

            string connStr = String.Format("server={0};user id={1}; password={2}; database={3}; pooling=false",
                server_config["ipaddr"], server_config["userid"], server_config["password"], server_config["db"]);

            try
            {
                conn = new MySqlConnection(connStr);
                conn.Open();
            }
            catch (MySqlException ex)
            {
                return false;
            }
            return true;
        }

        public bool insert(string text, ulong fingerprints)
        {
            MySqlConnection conn = new MySqlConnection();
            if (connect(_server_config, ref conn))
            {
                MySqlCommand cmd = new MySqlCommand(generate_query_sql(fingerprints), conn);
                try
                {
                    MySqlDataReader data_reader = cmd.ExecuteReader();
                    Dictionary<int, ulong> id_dict = new Dictionary<int, ulong>();
                    while (data_reader.Read())
                    {
                        id_dict.Add(data_reader.GetInt32(0), data_reader.GetUInt64(1));
                    }
                    data_reader.Close();

                    if (id_dict.Count() != 0)
                    {
                        //get the haming distance by diff
                        foreach (var item in id_dict)
                        {
                            if (is_similarity(fingerprints, item.Value))
                            {
                                return false;
                            }
                        }
                    }
                    //query is over & no similarity fingerprints, can insert the record
                    cmd = new MySqlCommand(generate_insert_sql(text, fingerprints), conn);
                    cmd.ExecuteNonQuery();
                }
                catch (MySqlException ex)
                {
                    Console.WriteLine("ERROR occured at insert, MESSAGE:" + ex);
                }
                finally
                {
                    conn.Close();
                }
                return true;
            }
            return false;
        }

        private bool is_similarity(ulong source, ulong target)
        {
            ulong x = (source ^ target) & (((1UL << _count_bits - 1) - 1) ^ (1UL << _count_bits - 1));
            int tot = 0;
            while (x > 0)
            {
                tot += 1;
                if (tot > 3)
                {
                    return false;
                }
                x &= x - 1;
            }
            if (tot > 3)
            {
                return false;
            }
            return true;
        }

        private string generate_insert_sql(string text, ulong fingerprints)
        {
            //todo
            Dictionary<string, ulong> combine_cols_insert = new Dictionary<string,ulong>();
            foreach (var item in _combine_bit_mask)
            {
                combine_cols_insert.Add(string.Format("combine_{0}", item.Key), (item.Value & fingerprints));
                //Console.WriteLine(string.Format("{0}, {1:x}, {2:x}", item.Key, item.Value, (item.Value & fingerprints)));
            }
            return "INSERT INTO corpus(article, " +
                "fingerprints, " + 
                string.Join(",", combine_cols_insert.Keys) +
                ") VALUES ('" +
                text +"', '" + 
                fingerprints + "', '" + 
                string.Join("', '", combine_cols_insert.Values) + "');";
        }

        private string generate_query_sql(ulong fingerprints)
        {
            List<string> combine_cols_query = new List<string>();
            foreach (var item in _combine_bit_mask)
            {
                combine_cols_query.Add(string.Format("combine_{0} = {1}", item.Key, (item.Value&fingerprints)));
            }
            return "SELECT id, fingerprints FROM corpus WHERE " + string.Join(" OR ", combine_cols_query) + ";";
        }

        #endregion
    }
}
