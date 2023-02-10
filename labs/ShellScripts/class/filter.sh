echo "creating the Directoty shell-scripts"
mkdir shell-scripts

echo "Directory shell-scripts Created"
touch /shell-scripts/cities.txt
echo "Created cities.txt"

echo "Enter Cities:"
read city
cat >> /shell-scripts/cities.txt
$city

grep "*" "/shell-scripts/cities.txt"| sed 's/New/Old/gi' > /shell-scripts/oldcities.txt
