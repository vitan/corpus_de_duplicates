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
        /// 
        /// </summary>
        private MySqlConnection _conn;
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
            connect(_server_config);
        }
        public void reset()
        {
            if (connect(_server_config))
            {
                List<string> sql_cols = new List<string>();
                List<string> sql_indexes = new List<string>();
                foreach (IList<int> item in _combinations)
                {
                    string com = getstring<int>(item);
                    sql_cols.Add(string.Format("{0} BIGINT NOT NULL", "combine_" + com));
                    sql_indexes.Add(string.Format("CREATE INDEX {0} ON corpus({1}); ", "index_"+com, "combine_"+com));
                }

                string sql = "DROP TABLE IF EXISTS `corpus`; " +
                    "CREATE TABLE corpus( " +
                    "id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, " +
                    "text TEXT CHARACTER SET utf8 NOT NULL, " +
                    "fingerprints BIGINT NOT NULL, " +
                    string.Join(", ", sql_cols.ToArray())+ ");" +
                    string.Join("", sql_indexes.ToArray());
                MySqlCommand cmd = new MySqlCommand(sql, _conn);
                try
                {
                    cmd.ExecuteNonQuery();
                }
                catch(MySqlException ex)
                {
                    Console.WriteLine("ERROR occured at create tables, MESSAGE:" + ex);
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

        private bool connect(Dictionary<string, string> server_config)
        {
            if (_conn != null)
                _conn.Close();

            string connStr = String.Format("server={0};user id={1}; password={2}; database={3}; pooling=false",
                server_config["ipaddr"], server_config["userid"], server_config["password"], server_config["db"]);

            try
            {
                _conn = new MySqlConnection(connStr);
                _conn.Open();
            }
            catch(MySqlException ex)
            {
                return false;
            }
            return true;
        }

        public bool insert(ulong fingerprints)
        {
            MySqlDataReader data_reader;
            MySqlCommand cmd = new MySqlCommand(generate_query(fingerprints), _conn);
            try
            {
                data_reader = cmd.ExecuteReader();
                data_reader.Read();
            }
            catch (MySqlException ex)
            {
                Console.WriteLine("ERROR occured at create tables, MESSAGE:" + ex);
            }

            Console.WriteLine(string.Format("I'm {0}", generate_query(fingerprints)));
            return true;
        }
        private string generate_query(ulong fingerprints)
        {
            List<string> combine_cols_query = new List<string>();
            foreach (var item in _combine_bit_mask)
            {
                combine_cols_query.Add(string.Format("combine_{0} = {1}", item.Key, (item.Value&fingerprints)));
            }
            return "SELECT id FROM corpus WHERE " + string.Join(" OR ", combine_cols_query) + ";";
        }

        public int find()
        {
            return 0;
        }
        #endregion
    }
}
