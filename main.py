from pyspark import SparkContext
from PIL import Image

sc = SparkContext()
img = Image.open("image.jpg")
tmp_pixel = list(img.getdata())
pixels = [tmp_pixel[i:i+img.width] for i in range(0, len(tmp_pixel), img.width)]

def blur(pixel):
    i, j = pixel[0], pixel[1]

    surrounding_pixels = []
    for x in range(i-1, i+2):
        for y in range(j-1, j+2):
            if x >= 0 and x < len(pixels) and y >= 0 and y < len(pixels[0]):
                surrounding_pixels.append(pixels[x][y])

    r_sum = 0
    g_sum = 0
    b_sum = 0

    for p in surrounding_pixels:
        r_sum += p[0]
        g_sum += p[1]
        b_sum += p[2]

    average = (r_sum/len(surrounding_pixels), g_sum/len(surrounding_pixels), b_sum/len(surrounding_pixels))

    return (i, j, average)


# Create RDD of all pixels
pixels_rdd = sc.parallelize( [(i, j) for i in range(len(pixels)) for j in range(len(pixels[i])) ])
blurred_pixels_rdd = pixels_rdd.map(blur)
blurred_pixels = blurred_pixels_rdd.collect()

blurred_image = Image.new("RGB", (img.width, img.height))
blurred_image.putdata([(int(r), int(g), int(b)) for i, j, (r, g, b) in blurred_pixels])
blurred_image.save("blurred_image.jpg")

sc.stop()
