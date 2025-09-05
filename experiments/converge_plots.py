import sys

import utils.eval_util as eval_util


def converge_plots(plot_script_file, plot_name, data_files):
    with open(plot_script_file, 'w') as f:
        f.write("set datafile separator ','\n")
        f.write("set key top left\n")
        f.write("set xlabel '%s'\n" % "Throughput (txns/sec)")
        f.write("set ylabel '%s'\n" % "Latency (ms)")
        f.write("set terminal pngcairo size %d,%d enhanced font '%s'\n" %
                (800,  # config['plot_tput_lat_png_width']
                 600,  # config['plot_tput_lat_png_height']
                 "DejaVu Sans,12",  # config['plot_tput_lat_png_font']
                 ))
        f.write('set output "%s"\n' % plot_name)
        args = []
        for i in range(len(data_files)):
            args.append("'%s' title '%s' with linespoint" %
                        (data_files[i], "Line " + str(i)))
        f.write("plot " + ",\\\n".join(args))

    eval_util.run_gnuplot(data_files, plot_name, plot_script_file)


def main():
    if len(sys.argv) < 4:
        sys.stderr.write(
            "Usage: python3 %s <plot_script_file> <plot_name_file> <csv_data_files>\n" %
            sys.argv[0])
        sys.exit(1)

    converge_plots(sys.argv[1], sys.argv[2], sys.argv[3:])


if __name__ == "__main__":
    main()
