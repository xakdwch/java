#! /bin/bash                                 

#get paramter from command line                                             
dev=eth0                                     
average=1                                    
band_alarm=0                                 
                                             
if [ $# -lt 1 ] || [ $# -gt 3 ] ; then       
        echo "Usage: ./net_statistic.sh eth0"
        exit 0                      
fi                                  
                                                                                
dev=$1                                                                          
                                                                                
if [ $# -gt 1 ]; then                                                           
        average=$2                                                              
fi                                                                              
                                                                                
if [ $# -gt 2 ]; then                                                           
        band_alarm=$3                                                           
fi                                                                              
                                                                                
                                                                                
                                                                                   
typeset in in_old dif_in dif_in1                                                    
typeset out out_old dif_out dif_out1  

in_average=0                                                                        
out_average=0
win=0                                                  
                                                                                    
in_old=$(cat /proc/net/dev | grep ${dev} | sed 's=^.*:==' | awk '{ print $1 }' )    
out_old=$(cat /proc/net/dev | grep ${dev} | sed 's=^.*:==' | awk '{ print $9 }')    
                                                                                    
while true                                                                      
do                                                                              
        sleep 1                                                                 
        time_str=$(date)                                                        
        in=$(cat /proc/net/dev | grep ${dev} | sed 's=^.*:==' | awk '{ print $1 }')
        out=$(cat /proc/net/dev | grep ${dev} | sed 's=^.*:==' | awk '{ print $9 }')
        dif_in=$(((in-in_old)*8))                                                   
        dif_in1=$((dif_in/1024/1024))                                               
        dif_out=$(((out-out_old)*8))                                                
        dif_out1=$((dif_out/1024/1024))                                             
        in_old=${in}                                                                
        out_old=${out}                                                              
        if [ $average -eq 1 ]; then                                                 
                echo "[${time_str}] IN: ${dif_in}Bps(${dif_in1}Mbps)  OUT: ${dif_out}Bps(${dif_out1}Mbps)"
        fi                                                                                                
                                                                                                          
        if [ $average -gt 1 ]; then                                                                       
                if [ $win -lt $average ]; then                                                            
                        win=$((win+1))                                                                    
                fi                                                                                        
                in_average=$(((in_average*(win-1)+dif_in1)/win))                                          
                out_average=$(((out_average*(win-1)+dif_out1)/win))                                       
                out_diff=$((out_average*1024*1024))
                
		echo "[${time_str}] IN: ${dif_in}Bps(${in_average}Mbps)  OUT: ${out_diff}Bps(${out_average}Mbps)"       
        fi                                                                                                
                                                                                                          
        if [ $band_alarm -gt 0 ]; then                                                                    
                if [ $((in_average-dif_in1)) -gt $band_alarm ] || [ $((dif_in1-in_average)) -gt $band_alarm ]; then
                        echo "************************************************************************IN bandwide change exceed ${band_alarm}Mbps"
                fi                                                                                                                  
                                                                                                                                    
                if [ $((out_average-dif_out1)) -gt 100 ] || [ $((dif_out1-out_average)) -gt 100 ]; then                             
                        echo "************************************************************************OUT bandwide change exceed ${band_alarm}Mbps"
                fi                                                                                                                  
        fi                                                                                                                          
done 
