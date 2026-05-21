echo "#-----------------# Compiling utils #-----------------#"
call compile-utils.bat
cd ..\scripts-windows

echo "#-----------------# Compiling group-definer #-----------------#"
call compile-gd.bat
cd ..\scripts-windows

echo "#-----------------# Compiling processing-node #-----------------#"
call compile-pn.bat
cd ..\scripts-windows

echo "#-----------------# Compiling mobile-node #-----------------#"
call compile-mn.bat
cd ..\scripts-windows
