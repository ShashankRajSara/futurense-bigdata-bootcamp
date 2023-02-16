echo "Choose From the Below:"
echo "1-Add, 2-Sub, 3-Mul, 4-Div"

read option

echo "Enter 1st number: "
read a

echo "Enter 2st number: "
read b


if [ "$option"  = 1 ]
then
    echo "SUM is: `expr $a + $b`"
elif [ "$option" = 2 ]
then
    echo "SUB is: `expr $a - $b`"
elif [ "$option" = 3 ]
then
    echo "MUL is: `expr $a \* $b`"
else
    echo "DIV is: `expr $a / $b`"
fi