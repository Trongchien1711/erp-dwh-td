import zipfile, re

with zipfile.ZipFile(r"C:\Users\bus_an\Desktop\Tổng Hợp PPW\Báo-Cáo-Thử-Việc.pptx") as z:
    texts=[]
    for f in z.namelist():
        if f.startswith("ppt/slides/") and f.endswith(".xml"):
            texts.extend(re.findall(r"<a:t[^>]*>(.*?)</a:t>", z.read(f).decode("utf-8")))

with open(r"d:\Data Warehouse\pptx_texts.txt", "w", encoding="utf-8") as f:
    f.write("\n".join(texts))
