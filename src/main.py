import sys
import argparse
from test.alaiza_project.MAIN import main_zurich
from test.libs.logger import specific_logger

sys.path.insert(0, 'src.zip')





def build_argument_parser():
    parser = argparse.ArgumentParser(description='Test_berlin_project_parser')
    parser.add_argument("--step", required=True, type=int, default=1,choices=[1,2,3,4],
                        help="""this is the selector of steps to execute, you can select 1,2,3,4 or 5 
                        having the following:
                                    1- preprocess
                                    2- Normalize
                                    3- Extract
                                    4- Integrate
                                    5- Enrich
                        **Notice that if you select one number, all the lower choice will be executed previously 
                        """)
    parser.add_argument("--file", required=True, type=str,
                        help="""Name of the file on input folder (extension included)""")
    return parser



def main():
    try:
        logger = specific_logger()
        parser = build_argument_parser()
        arguments = vars(parser.parse_args())
        main_zurich(arguments, logger)

    except Exception, ex:
        print ex


if __name__ == "__main__":
    main()

