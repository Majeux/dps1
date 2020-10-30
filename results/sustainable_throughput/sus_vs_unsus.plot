set title "Sustainable vs. unsustainable throughput for 2 worker nodes"
set xrange [0:500]
set xlabel "Time (s)"
set ylabel "Latency"
set output 'sus_vs_unsus.png'
set terminal png size 1024,768
plot '2nodes_13000ipm.res' title "Sustainable (13K/s)" with lines lw 3, '2nodes_14000ipm.res' title "Unsustainable (14K/s)" with lines lw 3
