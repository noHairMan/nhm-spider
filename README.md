### NHM-SPIDER

> 使用asyncio异步方式实现的爬虫，写法与scrapy相同。

##### 待完成

- 爬虫结束后的统计数据的展示，items，errors，200的次数等。
- 爬虫开始时开启的模块的展示，middleware，pipeline等。

##### 发布

```bash
# 更新版本号在 pyproject.toml 中
# 然后执行：

# 构建分发包
uv run build

# 上传到 PyPI
uv run twine upload dist/*
```

- @auther: noHairMan
- @time: 2021-03-18