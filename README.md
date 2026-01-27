# 



## sync to server

```shell
rsync -avz \
    --dry-run \
    --delete \
    --exclude-from=.gitignore \
    --exclude config.ini \
    --exclude .git \
    ../iotdb-py_client \
    root@11.101.17.124:~/
```

