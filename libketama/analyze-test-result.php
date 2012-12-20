<?php

$hashes = array();
$addresses = array();
$collisions = 0;

while ($line = fgets(STDIN, 8192)) {
    $line = trim($line);
    if (!$line) continue;
    list($hash, $point, $address) = explode(" ", $line);
    if (isset($hashes[$hash])) {
        $collisions++;
    }
    else {
        $hashes[$hash] = true;
    }
    $addresses[$address] += 1;
}

print "Collisions: $collisions\n\n";
ksort($addresses);
foreach ($addresses as $address => $count) {
    print "$address\t$count\n";
}

?>
