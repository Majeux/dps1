set title plottitle
set xlabel "Time (s)"
set ylabel "Latency"
set yrange [0:20]
set output 'img/out.png'
set terminal png size 1024,512
set nokey 
plot filename title plottitle with lines lw 3
