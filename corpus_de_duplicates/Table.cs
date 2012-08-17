using System;
using System.Collections.Generic;
using System.Collections;
using System.Linq;
using System.Text;
using MySql.Data.MySqlClient;

namespace corpus_de_duplicates
{
    struct Article
    {
        public int id;
        public string title;
        public int state;
        public string translator;
        public int count;
    }
    struct Original
    {
        public int id;
        public Dictionary<string, ulong> sentences_fingerprint;
    }
    struct Translation
    {
        public int id;
        public Dictionary<string, ulong> sentences_fingerprint;
    }
    struct Link
    {
        public Article article;
        public Original original;
        public Translation translation;
    }
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

        #region sentences de_duplicates_related functions
        public void reset_tables_exclude_corpus()
        {
            MySqlConnection conn = new MySqlConnection();
            if (connect(_server_config, ref conn))
            {
                #region //the following tables constrain the table 'corpus'
                List<string> sql_cols = new List<string>();
                List<string> sql_indexes_original = new List<string>();
                List<string> sql_indexes_translation = new List<string>();
                foreach (IList<int> item in _combinations)
                {
                    string com = getstring<int>(item);
                    sql_cols.Add(string.Format("{0} BIGINT UNSIGNED NOT NULL", "combine_" + com));
                    sql_indexes_original.Add(string.Format("CREATE INDEX {0} ON original({1}); ", "index_" + com, "combine_" + com));
                    sql_indexes_translation.Add(string.Format("CREATE INDEX {0} ON translation({1}); ", "index_" + com, "combine_" + com));
                }
                string sql_create_article = "Drop TABLE IF EXISTS `article`; " +
                    "CREATE TABLE article(article_id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, title VARCHAR CHARACTER SET utf8, state INT NOT NULL, translator VARCHAR CHARACTER SET utf8, sentence_count INT NOT NULL);";
                string sql_create_original = "Drop TABLE IF EXISTS `original`; " +
                    "CREATE TABLE original(original_id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, sentence TEXT CHARACTER SET utf8 NOT NULL, fingerprints BIGINT UNSIGNED NOT NULL, " +
                    string.Join(", ", sql_cols.ToArray()) + ");" +
                    string.Join(", ", sql_indexes_original.ToArray()) + ");";
                string sql_create_translation = "Drop TABLE IF EXISTS `translation`; " +
                    "CREATE TABLE translation(translation_id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, sentence TEXT CHARACTER SET utf8 NOT NULL, fingerprints BIGINT UNSIGNED NOT NULL, " +
                    string.Join(", ", sql_cols.ToArray()) + ");" +
                    string.Join(", ", sql_indexes_translation.ToArray()) + ");";
                string sql_create_link = "Drop TABLE IF EXISTS `link`; " +
                    "CREATE TABLE link(id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, article_id INT NOT NULL, original_id INT NOT NULL, original_order INT NOT NULL, translation_id INT, " +
                    "FOREIGN KEY(article_id) REFERENCES article(article_id), FOREIGN KEY(original) REFERENCES original(original_id), FOREIGN KEY(translation) REFERENCES translation(translation_id));";
                #endregion

                MySqlCommand cmd = new MySqlCommand(sql_create_article + sql_create_original + sql_create_translation + sql_create_link, conn);
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

        public List<string> query_from_corpus()
        {
            List<string> files = new List<string>();
            MySqlConnection conn = new MySqlConnection();
            if (connect(_server_config, ref conn))
            {
                MySqlCommand cmd = new MySqlCommand("SELECT article from corpus", conn);
                try
                {
                    MySqlDataReader data_reader = cmd.ExecuteReader();
                    while (data_reader.Read())
                    {
                        files.Add(data_reader.GetString(0));
                    }
                    data_reader.Close();
                }
                catch (MySqlException ex)
                {
                    Console.WriteLine("ERROR occured at insert, MESSAGE:" + ex);
                }
                finally
                {
                    conn.Close();
                }
            }
            return files;
        }

        public bool insert_sentences(Link link)
        {

            return false;
        }
        #endregion

        #region text de_duplicates_related functions
        public void reset_table_corpus()
        {
            MySqlConnection conn = new MySqlConnection();
            if (connect(_server_config, ref conn))
            {
                #region //table corpus contains all the de_duplicates_corpus path, PLEASE DON'T RUN IT AGAIN， RUN IT IS DANGEROUS!!!!
                List<string> sql_cols = new List<string>();
                List<string> sql_indexes = new List<string>();
                foreach (IList<int> item in _combinations)
                {
                    string com = getstring<int>(item);
                    sql_cols.Add(string.Format("{0} BIGINT UNSIGNED NOT NULL", "combine_" + com));
                    sql_indexes.Add(string.Format("CREATE INDEX {0} ON corpus({1}); ", "index_" + com, "combine_" + com));
                }
                string sql = "DROP TABLE IF EXISTS `corpus`; " +
                    "CREATE TABLE corpus( " +
                    "id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, " +
                    "article TEXT CHARACTER SET utf8 NOT NULL, " +
                    "fingerprints BIGINT UNSIGNED NOT NULL, " +
                    string.Join(", ", sql_cols.ToArray()) + ");" +
                    string.Join("", sql_indexes.ToArray());
                #endregion

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

        public bool insert_corpus(string text, ulong fingerprints)
        {
            MySqlConnection conn = new MySqlConnection();
            if (connect(_server_config, ref conn))
            {
                MySqlCommand cmd = new MySqlCommand(generate_query_sql_corpus(fingerprints), conn);
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
                    //query is over & no similarity fingerprints, can insert_corpus the record
                    cmd = new MySqlCommand(generate_insert_sql_corpus(text, fingerprints), conn);
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

        private string generate_insert_sql_corpus(string text, ulong fingerprints)
        {
            //todo
            Dictionary<string, ulong> combine_cols_insert = new Dictionary<string,ulong>();
            foreach (var item in _combine_bit_mask)
            {
                combine_cols_insert.Add(string.Format("combine_{0}", item.Key), (item.Value & fingerprints));
            }
            return "INSERT INTO corpus(article, " +
                "fingerprints, " + 
                string.Join(",", combine_cols_insert.Keys) +
                ") VALUES ('" +
                text +"', '" + 
                fingerprints + "', '" + 
                string.Join("', '", combine_cols_insert.Values) + "');";
        }

        private string generate_query_sql_corpus(ulong fingerprints)
        {
            List<string> combine_cols_query = new List<string>();
            foreach (var item in _combine_bit_mask)
            {
                combine_cols_query.Add(string.Format("combine_{0} = {1}", item.Key, (item.Value&fingerprints)));
            }
            return "SELECT id, fingerprints FROM corpus WHERE " + string.Join(" OR ", combine_cols_query) + ";";
        }
        #endregion

        #endregion
    }
}
