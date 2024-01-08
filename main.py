import argparse
import os

from parse_goods_info import AmazonParserInfo
from parse_rubricks import AmazonParseLinks
import asyncio


def run_parse_goods(file_path_read: str = None, json_file_path_save: str = None, max_requests: int = 10):
    amaz_parse = AmazonParserInfo(file_path=file_path_read,
                                  json_file_path=json_file_path_save)
    asyncio.run(amaz_parse.process_links(max_concurrent_requests=max_requests))


def run_rubrics(max_requests: int = None, missed_data: bool = False, file_path_save: str = None, department: str = None,
                step: float = None, min_price: int = None, max_price: int = None):
    amaz_parse = AmazonParseLinks(department=department, step=step, rubric=rubric, min_price=min_price,
                                  max_price=max_price,
                                  file_path_save=file_path_save)
    asyncio.run(amaz_parse.process_links(max_concurrent_requests=max_requests, missed_data=missed_data))


if __name__ == '__main__':

    argparser = argparse.ArgumentParser(description='Input arguments')
    argparser.add_argument('--file_path_read', type=str, default='Amazon_links_data.xlsx',
                           help='file name with path to this file for saving data')
    argparser.add_argument('--json_file_path_save', type=str, default='json_parsed_files/amazon_products.json',
                           help='json_file_path')
    argparser.add_argument('--missed_data', type=bool, default=False, required=False)
    argparser.add_argument('--max_requests', type=int, default=10, required=False)
    argparser.add_argument('--step', type=float, default=0.2, required=False)
    argparser.add_argument('--min_price', type=int, default=1, required=False)
    argparser.add_argument('--max_price', type=int, default=5000, required=False)
    argparser.add_argument('--rubric', type=str, default="bbn=7141123011&rh=n%3A2368365011%2Cp_36%3A")
    argparser.add_argument('--department', type=str, default="fashion-womens-clothing")

    argparser.add_argument('--act', type=str, required=True, choices=['run_rubrics', 'run_parse_goods'],
                           help='action')
    argparser.add_argument('--root', type=str, default=os.getcwd())
    args, unknown = argparser.parse_known_args()

    if not os.path.exists(os.path.join(args.root, 'json_parsed_files/')):
        os.mkdir(os.path.join(args.root, 'json_parsed_files/'))
    if not os.path.exists(os.path.join(args.root, 'excel_parsed_files/')):
        os.mkdir(os.path.join(args.root, 'excel_parsed_files/'))

    if args.act == 'run_rubrics':
        run_rubrics(max_requests=args.max_requests, missed_data=args.missed_data, file_path_save=args.file_path_read,
                    department=args.department, step=args.step, rubric=args.rubric,
                    min_price=args.min_price, max_price=args.max_price, )
    elif args.act == 'run_parse_goods':
        run_parse_goods(args.file_path_read, args.json_file_path_save, args.max_requests)
    else:
        raise ValueError(f'Do not know action {args.act}')
