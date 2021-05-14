# json_streamer
*a library to read in large JSON lists while conserving memory*

```
Example usage:

    from json_streamer import JSONStreamer

    f = open('myfile.json')
    json_stream = JSONStreamer(f)

    for item in json_stream:
        print(item)
```
