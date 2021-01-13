for IFACE in eth1 eth2
do
  for CLIENT in $(wl -i $IFACE assoclist | sed 's/assoclist//')
  do
    STRENGTH=$(wl -i $IFACE rssi $CLIENT)
    echo $IFACE $CLIENT $STRENGTH
  done
done
