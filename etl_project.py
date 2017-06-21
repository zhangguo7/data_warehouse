# coding:utf-8


import sys, getopt

def main(argv):
    inputfile = ""
    outputfile = ""

    try:
        # 这里的 h 就表示该选项无参数，i:表示 i 选项后需要有参数
        opts, args = getopt.getopt(argv, "hi:o:",["infile=", "outfile="])
    except getopt.GetoptError:
        print 'Error: test_arg.py -i <inputfile> -o <outputfile>'
        print '   or: test_arg.py --infile=<inputfile> --outfile=<outputfile>'
        sys.exit(2)

    for opt, arg in opts:
        if opt == "-h":
            print 'test_arg.py -i <inputfile> -o <outputfile>'
            print 'or: test_arg.py --infile=<inputfile> --outfile=<outputfile>'

            sys.exit()
        elif opt in ("-i", "--infile"):
            inputfile = arg
        elif opt in ("-o", "--outfile"):
            outputfile = arg

    print 'Input file : ', inputfile
    print 'Output file: ', outputfile

if __name__ == "__main__":
    main(sys.argv[1:])