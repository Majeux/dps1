set title "Sustainable vs. unsustainable throughput for 4 worker nodes"
set xrange [0:400]
set xlabel "Time (s)"
set ylabel "Latency"
set output 'sus_vs_unsus.png'
set terminal png size 1024,768
set key left top
plot '4nodes_16000' title "16K/s (unsustainable)" with lines lw 3, '4nodes_15000' title "15K/s (sustainable)" with lines lw 3
