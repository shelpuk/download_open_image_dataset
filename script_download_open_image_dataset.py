import urllib
import threading
import time

def get_open_images(description_file, destination_folder, block_size, thread_id, load_autosave=False):
    checkpoint_file='/media/tassadar/Data/open_images/.lastimage_'
    with open(description_file, 'r') as f:
        with open(log_file+str(thread_id)+'.log', 'wb') as l:

            image = urllib.URLopener()
            offset = 0

            if load_autosave:
                with open(checkpoint_file+str(thread_id), 'r') as af:
                    lines = af.readlines()
                    offset = int(lines[0]) + 1

            num_images = offset

            line = f.readline()

            for _ in range(block_size*thread_id + offset):
                line = f.readline()

            while line != "" and num_images <= block_size:
                try:
                    params = line.split(',')
                    id = params[0]
                    url = params[2]

                    image.retrieve(url, destination_folder+id+'.jpg')
                    print 'Downloaded '+url+' as '+id+'.jpg'

                except:
                    l.write("Cannot retrieve:\n"+line+"\n\n")

                if num_images % 50 == 0:
                    with open(checkpoint_file+str(thread_id), 'w') as cf:
                        cf.write(str(num_images))

                num_images += 1
                line = f.readline()


description_file = "/media/tassadar/Data/open_images/description/images_2016_08/train/images.csv"
destination_folder = "/media/tassadar/Data/open_images/images/train/"
log_file = "/media/tassadar/Data/open_images/retrieval_train"

num_images = 9011219
num_threads = 16
load_autosave = True

block_size = num_images // num_threads + 1
threads = []

for n in range(num_threads):
    t = threading.Thread(target=get_open_images, args=(description_file,destination_folder,block_size,n,load_autosave))
    t.daemon = True
    t.start()
    threads.append(t)

while True:
    time.sleep(1000)

