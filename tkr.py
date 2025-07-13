import tkinter as tk
from tkinter import ttk
from pyspark.sql import SparkSession
from pyspark.sql.functions import lower, col
import webbrowser

def search_music(search_type, search_term, df):
    if search_type.lower() == "track":
        search_result = df.filter(lower(col('track_name')).contains(search_term.lower())).select('track_id').limit(1)
    elif search_type.lower() == "artist":
        search_result = df.filter(lower(col('artist_name')).contains(search_term.lower())).select('track_name', 'track_id')
        if search_result.count() == 0:
            result_label.config(text="Không tìm thấy nghệ sĩ hoặc không có bài hát của nghệ sĩ này.")
            return None
        else:
            result_label.config(text="Danh sách bài hát của nghệ sĩ:")
            search_result_text = ""
            for row in search_result.collect():
                search_result_text += row['track_name'] + "\n"
            result_text.config(state=tk.NORMAL)
            result_text.delete(1.0, tk.END)
            result_text.insert(tk.END, search_result_text)
            result_text.config(state=tk.DISABLED)
            selected_track_entry.config(state=tk.NORMAL)
            return
    else:
        result_label.config(text="Loại tìm kiếm không hợp lệ. Vui lòng nhập 'track' hoặc 'artist'.")
        return None

def open_spotify_track(track_name):
    if track_name:
        # Tìm kiếm ID của bài hát bằng tên
        search_result = df.filter(lower(col('track_name')).contains(track_name.lower())).select('track_id').limit(1)
        if search_result.count() > 0:
            track_id = search_result.collect()[0]['track_id']
            spotify_track_link = f"https://open.spotify.com/track/{track_id}"
            webbrowser.open(spotify_track_link)
        else:
            result_label.config(text="Không tìm thấy bài hát.")
    else:
        result_label.config(text="Không tìm thấy bài hát.")

def search():
    search_type = search_type_entry.get().lower()
    search_term = search_term_entry.get()
    if search_type == "track" or search_type == "artist":
        search_music(search_type, search_term, df)
    else:
        result_label.config(text="Loại tìm kiếm không hợp lệ. Vui lòng nhập 'track' hoặc 'artist'.")

# Khởi tạo SparkSession
spark = SparkSession.builder \
    .appName("MusicSearchApp") \
    .getOrCreate()

# Đọc dữ liệu từ tệp CSV
df = spark.read.option("header", True).csv("spotify_data.csv")

#----------Tạo giao diện Tkinter--------------------------
# Tạo cửa sổ tkinter
root = tk.Tk()
root.title("Music Search App")

# Tạo các thành phần giao diện
search_type_label = tk.Label(root, text="Bạn muốn tìm kiếm theo 'track' hay 'artist'?: ")
search_type_label.grid(row=0, column=0, sticky="w")
search_type_entry = tk.Entry(root)
search_type_entry.grid(row=0, column=1)

search_term_label = tk.Label(root, text="Nhập tên bài hát hoặc nghệ sĩ bạn muốn tìm: ")
search_term_label.grid(row=1, column=0, sticky="w")
search_term_entry = tk.Entry(root)
search_term_entry.grid(row=1, column=1)

search_button = tk.Button(root, text="Tìm kiếm", command=search)
search_button.grid(row=2, column=0, columnspan=2)

result_label = tk.Label(root, text="")
result_label.grid(row=3, column=0, columnspan=2)

result_text = tk.Text(root, height=5, width=50)
result_text.grid(row=4, column=0, columnspan=2)

selected_track_label = tk.Label(root, text="Nhập tên bài hát bạn muốn mở: ")
selected_track_label.grid(row=5, column=0, sticky="w")
selected_track_entry = tk.Entry(root, state=tk.NORMAL)
selected_track_entry.grid(row=5, column=1)

open_button = tk.Button(root, text="Mở bài hát trên Spotify", command=lambda: open_spotify_track(selected_track_entry.get()))
open_button.grid(row=6, column=0, columnspan=2)

# Chạy ứng dụng
root.mainloop()

# Đóng SparkSession
spark.stop()
