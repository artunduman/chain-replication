file=test/checker_out.txt

printf "Test index -> $1\n" >> $file
../cs416_2021w2/assign3/spec/amm \
../cs416_2021w2/assign3/spec/a3spec.sc -n 10 trace_output.log >> $file
printf "\n" >> $file
