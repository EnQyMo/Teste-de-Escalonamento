echo "#-----------------# Compiling utils #-----------------#"
bash scripts/compile-utils.sh

echo "#-----------------# Compiling group-definer #-----------------#"
bash scripts/compile-gd.sh

echo "#-----------------# Compiling processing-node #-----------------#"
bash scripts/compile-pn.sh

echo "#-----------------# Compiling mobile-node #-----------------#"
bash scripts/compile-mn.sh
