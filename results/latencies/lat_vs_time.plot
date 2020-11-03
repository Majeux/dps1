set title plottitle
set xlabel "Time (s)"
set ylabel "Latency"
set output 'out.png'
set terminal png size 1024,512
set nokey 
plot filename title plottitle with lines lw 3
