<?php
function curlrequest($url,$file){
    $ch = curl_init(); 
    curl_setopt($ch, CURLOPT_URL, $url); 
    curl_setopt($ch, CURLOPT_RETURNTRANSFER,1); 
    curl_setopt($ch, CURLOPT_CUSTOMREQUEST, $method); 
    curl_setopt($ch, CURLOPT_INFILE, $file); 
    curl_setopt($ch, CURLOPT_INFILESIZE, filesize($file)); 
    $document = curl_exec($ch);
    if(!curl_errno($ch)){ 
      $info = curl_getinfo($ch); 
      echo 'Took ' . $info['total_time'] . ' seconds to send a request to ' . $info['url']; 
    } else { 
      echo 'Curl error: ' . curl_error($ch); 
    }
    curl_close($ch);
    return $document;
}
$url = "http://127.0.0.1:2380/mdb/";
for($i = 0; $i = 1000000; $i++)
{
    $str = "random data:";
    for($j = 0; $j < 100; $j++)
    {
        $str .= rand();
    }
    $file = "/tmp/infile";
    file_put_contents($file, $str);
    $uri = $url."/".substr(md5_file($file), 0, 16);
    curlrequest($uri, $file);
    echo "$uri\n"; 
}
?>
