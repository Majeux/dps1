set title "Latency distribution"
set xlabel "Latency"
set ylabel "Frequency"
set xrange [0:20]
set output 'img/out.png'
set terminal png size 1024,768
set nokey 
set style fill solid 0.3

binwidth=0.1
bin(x,width)=width*floor(x/width)

plot 'lat_90%_4nodes.res' using (bin($1,binwidth)):(1.0) smooth freq with boxes
