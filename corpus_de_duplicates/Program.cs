using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;

namespace corpus_de_duplicates
{
    class Program
    {
        static void Main(string[] args)
        {
            #region basic config
            Dictionary<string, string> server_config = new Dictionary<string,string>();
            server_config.Add("ipaddr", "localhost");
            server_config.Add("userid", "root");
            server_config.Add("password", "root");
            server_config.Add("db", "translate");

            int count_bits = 64;
            int count_blocks = 6;
            int num_diff_bits = 3;
            #endregion

            List<ulong> bit_mask = new List<ulong>();
            bit_mask.Add(0xFFE0000000000000);
            bit_mask.Add(0x1FFC0000000000);
            bit_mask.Add(0x3FF80000000);
            bit_mask.Add(0x7FF00000);
            bit_mask.Add(0xFFE00);
            bit_mask.Add(0x1FF);

            //Table table = new Table(server_config, count_bits, count_blocks, num_diff_bits, bit_mask);
            
            //DateTime d1 = DateTime.Now;
            //load_data(table);
            //DateTime d2 = DateTime.Now;
            //TimeSpan ts1 = new TimeSpan(d1.Ticks);
            //TimeSpan ts2 = new TimeSpan(d2.Ticks);
            //TimeSpan ts = ts1.Subtract(ts2).Duration();

            //string dateDiff = ts.Days.ToString() + "天"
            //        + ts.Hours.ToString() + "小时"
            //        + ts.Minutes.ToString() + "分钟"
            //        + ts.Seconds.ToString() + "秒";
            //Console.WriteLine("{0}", dateDiff);

            test_fingerprint();
            Console.ReadLine();
        }
        static void test_fingerprint()
        {
            string file = @"E:\smtproject\corpus_collection\data\texttest\100001";
            using (FileStream fs = new FileStream(file, FileMode.Open))
            {
                using(StreamReader r = new StreamReader(fs))
                {
                    ulong fingerprint = generate_fingerprint(r.ReadToEnd(), 3, 64);
                    Console.WriteLine(String.Format("{0:x}", fingerprint));
                }
            }
        }
        static void load_data(Table table)
        {
            string hashfile = @"E:\smtproject\corpus_collection\data\after_simhash";
            table.reset();
            string contents = string.Empty;
            using (FileStream fs = new FileStream(hashfile, FileMode.Open))
            {
                using (StreamReader r = new StreamReader(fs))
                {
                    contents = r.ReadToEnd();
                }

                int count = 1;
                int sum = 1;
                foreach (string line in contents.Split('\n'))
                {
                    string[] item = line.Split(' ');
                    //using (FileStream fsl = new FileStream(item[0], FileMode.Open))
                    //{
                    //    using (StreamReader r = new StreamReader(fsl, Encoding.UTF8))
                    //    {
                            //if(table.insert(mysql_replace(r.ReadToEnd()), Convert.ToUInt64(item[1])))
                            if (table.insert(item[0], Convert.ToUInt64(item[1])))
                            {
                                count += 1;
                            }
                    //    }
                    //}
                    //Console.WriteLine(string.Format("{0}", sum));
                    sum += 1;
                }
                Console.WriteLine(string.Format("count: {0}, sum: {1}", count, sum));
            }
        }

        static string mysql_replace(string s)
        {
            return s.Replace("'", "");
        }

        static UInt64 generate_fingerprint(string strText, int ngram, int hashbits)
        {
            IList<string> ngram_token = generate_ngram_tokens(strText.Split(' ').ToList<string>(), ngram);
            List<int> v = new List<int>(new int[hashbits]);
            foreach (string item in ngram_token)
            {
                ulong t = generate_UInt64_HashCode(item);
                ulong bitmask = 0;
                for (int i = 0; i < hashbits; i++)
                {
                    bitmask = 1UL << i;
                    if ((t & bitmask) > 0)
                    {
                        v[i] += 1;
                    }
                    else
                        v[i] -= 1;
                }
            }

            ulong fingerprint = 0;
            for (int i = 0; i < hashbits; i++)
            {
                if (v[i] >= 0)
                    fingerprint += (1UL << i);
            }
            return fingerprint;
        }

        static List<string> generate_ngram_tokens(List<string> tokens, int ngram)
        {
            if (tokens.Count == 0)
                return null;
            List<string> ngram_token = new List<string>();
            if (tokens.Count <= ngram)
            {
                ngram_token.Add(string.Join(" ", tokens));
                return ngram_token;
            }

            for (int i = ngram - 1; i < tokens.Count; i++)
            {
                ngram_token.Add(string.Join(" ", tokens, i + 1 - ngram, ngram));
            }
            return ngram_token;
        }

        static UInt64 generate_UInt64_HashCode(string strText)
        {
            UInt64 hashCode = 0;
            if (!string.IsNullOrEmpty(strText))
            {
                //Unicode Encode Covering all characterset
                byte[] byteContents = Encoding.Unicode.GetBytes(strText);
                System.Security.Cryptography.SHA256 hash = new System.Security.Cryptography.SHA256CryptoServiceProvider();
                byte[] hashText = hash.ComputeHash(byteContents);
                //32Byte hashText separate
                //hashCodeStart = 0~7  8Byte
                //hashCodeMedium = 8~23  8Byte
                //hashCodeEnd = 24~31  8Byte
                //and Fold
                UInt64 hashCodeStart = BitConverter.ToUInt64(hashText, 0);
                UInt64 hashCodeMedium = BitConverter.ToUInt64(hashText, 8);
                UInt64 hashCodeEnd = BitConverter.ToUInt64(hashText, 24);
                hashCode = hashCodeStart ^ hashCodeMedium ^ hashCodeEnd;
            }
            return hashCode;
        }
    }
}
