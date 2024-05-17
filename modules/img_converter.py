import os
import time
import cv2
import numpy as np

from ingestaweb.settings import TEMP_DIR_IMAGES


def create_overlayed_images(background_image_path, overlay_img_path, new_image_path):
    background = cv2.imread(background_image_path)
    overlay = cv2.imread(overlay_img_path, cv2.IMREAD_UNCHANGED)

    # separate the alpha channel from the color channels
    alpha_channel = overlay[:, :, 3] / 255 # convert from 0-255 to 0.0-1.0
    overlay_colors = overlay[:, :, :3]

    # To take advantage of the speed of numpy and apply transformations to the entire image with a single operation
    # the arrays need to be the same shape. However, the shapes currently looks like this:
    #    - overlay_colors shape:(width, height, 3)  3 color values for each pixel, (red, green, blue)
    #    - alpha_channel  shape:(width, height, 1)  1 single alpha value for each pixel
    # We will construct an alpha_mask that has the same shape as the overlay_colors by duplicate the alpha channel
    # for each color so there is a 1:1 alpha channel for each color channel
    alpha_mask = np.dstack((alpha_channel, alpha_channel, alpha_channel))

    # The background image is larger than the overlay so we'll take a subsection of the background that matches the
    # dimensions of the overlay.
    # NOTE: For simplicity, the overlay is applied to the top-left corner of the background(0,0). An x and y offset
    # could be used to place the overlay at any position on the background.
    h, w = overlay.shape[:2]
    background_subsection = background[0:h, 0:w]

    # combine the background with the overlay image weighted by alpha
    composite = background_subsection * (1 - alpha_mask) + overlay_colors * alpha_mask

    # overwrite the section of the background image that has been updated
    background[0:h, 0:w] = composite

    cv2.imwrite(new_image_path, background)


def build_pages(total_pages, mounted_images_path):
    status = False
    try:
        files = os.listdir(TEMP_DIR_IMAGES)
        unique_files = list(set([x.split('_')[0] for x in files if '.' in x]))
        if len(unique_files) == total_pages-1:
            unique_files.sort()
            [create_overlayed_images(
                os.path.join(TEMP_DIR_IMAGES, f'{x}_bg.jpeg'), os.path.join(TEMP_DIR_IMAGES, f'{x}_fg.png'),
                os.path.join(mounted_images_path, f'{x}.png'))
             for x in unique_files
            ]
            time.sleep(5)
            status = True
        else:
            raise Exception("Pages missing!!")
    except Exception as e:
        print(e)
    finally:
        return status


# def test_image_overlay(bg, ovl):
#     background = cv2.imread(bg,)
#     overlay = cv2.imread(ovl,
#                          cv2.IMREAD_UNCHANGED)  # IMREAD_UNCHANGED => open image with the alpha channel
#     height, width = overlay.shape[:2]
#     for y in range(height):
#         for x in range(width):
#             overlay_color = overlay[y, x, :3]  # first three elements are color (RGB)
#             overlay_alpha = overlay[y, x, 3] / 255  # 4th element is the alpha channel, convert from 0-255 to 0.0-1.0
#             # get the color from the background image
#             background_color = background[y, x]
#             # combine the background color and the overlay color weighted by alpha
#             composite_color = background_color * (1 - overlay_alpha) + overlay_color * overlay_alpha
#             # update the background image in place
#             background[y, x] = composite_color
#     cv2.imwrite('combined.png', background)

#
# if __name__ == "__main__":
#     create_overlayed_images(
#         background_image_path=r"C:\Users\christian.escudero\Downloads\bg_image_test.jpeg",
#         overlay_img_path=r"C:\Users\christian.escudero\Downloads\fg_image_test.png",
#         new_image_path="test_img_overlay.png"
#     )