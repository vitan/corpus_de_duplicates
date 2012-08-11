using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

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

            foreach (ulong item in bit_mask)
            {
                Console.WriteLine("{0:x}", item);
            }
            Table table = new Table(server_config, count_bits, count_blocks, num_diff_bits, bit_mask);
            table.reset();
        }
    }
}
