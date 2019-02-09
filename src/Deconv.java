import org.apache.commons.math3.complex.Complex;


public class Deconv {

    public static double[] deconvolution(double[] img, int img_frames, int img_height, int img_width,
                                     double[] psf, int psf_frames, int psf_height, int psf_width) {

        Complex[] h = preparePSF(psf, psf_frames, psf_height, psf_width, img_frames, img_height, img_width);

        int N = (int) Math.pow(2, Math.ceil(Math.log(img_frames * img_height * img_width) / Math.log(2)));
        double[] new_img = new double[N];
        System.arraycopy(img, 0, new_img, 0, img_frames * img_height * img_width);
        for (int i = img.length; i < N; i++) {
            new_img[i] = 1;
        }
        Complex[] new_h = new Complex[N];
        for (int i = 0; i < N; i++) {
            if (i < h.length) {
                new_h[i] = h[i];
            } else {
                new_h[i] = new Complex(0);
            }
        }

        RichardsonLucy rl = new RichardsonLucy();
        double[] rst = rl.run(new_img, N, new_h, 5);
        return rst;
    }


    private static void padding(double[] dst, int dst_frames, int dst_height, int dst_width,
                                double[] src, int src_frames, int src_height, int src_width) {
        int diff_frames = (dst_frames - src_frames) / 2;
        int diff_height = (dst_height - src_height) / 2;
        int diff_width = (dst_width - src_width) / 2;

        int min_frames = dst_frames > src_frames ? src_frames : dst_frames;
        int min_height = dst_height > src_height ? src_height : dst_height;
        int min_width = dst_width > src_width ? src_width : dst_width;

        for (int frame = 0; frame < min_frames; frame++) {
            int dst_frame = diff_frames >= 0 ? frame + diff_frames : frame;
            int src_frame = diff_frames >= 0 ? frame : frame - diff_frames;

            for (int row = 0; row < min_height; row++) {
                int dst_row = diff_height >= 0 ? row + diff_height : row;
                int src_row = diff_height >= 0 ? row : row - diff_height;

                for (int col = 0; col < min_width; col++) {
                    int dst_col = diff_width >= 0 ? col + diff_width : col;
                    int src_col = diff_width >= 0 ? col : col - diff_width;

                    dst[dst_frame * dst_height * dst_width + dst_row * dst_width + dst_col] =
                            src[src_frame * src_height * src_width + src_row * src_width + src_col];
                }
            }
        }
    }


    private static void normalize(double[] h) {
        double sum = 0;
        for (int i = 0; i < h.length; i++) {
            sum += h[i];
        }
        for (int i = 0; i < h.length; i++) {
            h[i] = h[i] / sum;
        }
    }


    private static void shift(double[] buffer, int len) {
        double[] temp = new double[len];
        System.arraycopy(buffer, 0, temp, 0, len);
        System.arraycopy(temp, len / 2, buffer, 0, len - len / 2);
        System.arraycopy(temp, 0, buffer, len - len / 2, len / 2);
        temp = null;
    }


    private static void circular(double[] h, int frames, int height, int width) {
        for (int col = 0; col < width; col++) {
            for (int row = 0; row < height; row++) {
                double[] buffer = new double[frames];
                for (int frame = 0; frame < frames; frame++) {
                    buffer[frame] = h[frame * height * width + row * width + col];
                }
                shift(buffer, frames);
                for (int frame = 0; frame < frames; frame++) {
                    h[frame * height * width + row * width + col] = buffer[frame];
                }
                buffer = null;
            }
        }

        for (int col = 0; col < width; col++) {
            for (int frame = 0; frame < frames; frame++) {
                double[] buffer = new double[height];
                for (int row = 0; row < height; row++) {
                    buffer[row] = h[frame * height * width + row * width + col];
                }
                shift(buffer, height);
                for (int row = 0; row < height; row++) {
                    h[frame * height * width + row * width + col] = buffer[row];
                }
                buffer = null;
            }
        }

        for (int row = 0; row < height; row++) {
            for (int frame = 0; frame < frames; frame++) {
                double[] buffer = new double[width];
                for (int col = 0; col < width; col++) {
                    buffer[col] = h[frame * height * width + row * width + col];
                }
                shift(buffer, width);
                for (int col = 0; col < width; col++) {
                    h[frame * height * width + row * width + col] = buffer[col];
                }
                buffer = null;
            }
        }
    }


    private static Complex[] preparePSF(double[] psf,
                                        int psf_frame, int psf_height, int psf_width,
                                        int img_frame, int img_height, int img_width) {

        double[] h = new double[img_frame * img_height * img_width];

        padding(h, img_frame, img_height, img_width, psf, psf_frame, psf_height, psf_width);

        normalize(h);

        circular(h, img_frame, img_height, img_width);

        Complex[] c_h = new Complex[h.length];
        for (int i = 0; i < h.length; i++) {
            c_h[i] = new Complex(h[i]);
        }
        return c_h;
    }

    public static void main(String[] args) {
        System.out.println("Deconv");
        int i1 = 57, i2 = 430, i3 = 290;
        int p1 = 80, p2 = 80, p3 = 80;
        double[] img = new double[i1 * i2 * i3];
        double[] psf = new double[p1 * p2 * p3];

        for (int i = 0; i < img.length; i++) {
            img[i] = Math.random() * 255;
        }
        for (int i = 0; i < psf.length; i++) {
            psf[i] = Math.random() * 65535;
        }

        deconvolution(img, i1, i2, i3, psf, p1, p2, p2);
    }
}
