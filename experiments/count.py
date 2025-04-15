import argparse
import sys

import numpy


def main():
    parser = argparse.ArgumentParser(description="parse args")
    parser.add_argument('logfile', type=str)
    parser.add_argument('--atomic', action='store_true')
    parser.add_argument('--search_phrases', type=str, nargs='+', required=True)
    parser.add_argument('--end_phrases', type=str, nargs='+')
    args = parser.parse_args()

    with open(args.logfile, 'r') as file:
        search_for_start_phrase = True
        start_time = 0
        results = []
        for line in file:
            if search_for_start_phrase:
                if all(phrase in line for phrase in args.search_phrases):
                    if not args.atomic:
                        search_for_start_phrase = False
                        start_time = int(line.strip().split()[-1])
                    else:
                        duration = int(line.strip().split()[-1])
                        results.append(duration)
            else:
                if all(phrase in line for phrase in args.end_phrases):
                    search_for_start_phrase = True
                    end_time = int(line.strip().split()[-1])
                    results.append(end_time - start_time)

    print("median: ", numpy.median(results))
    print("p99: ", numpy.percentile(results, 99))

    return 0


if __name__ == "__main__":
    sys.exit(main())