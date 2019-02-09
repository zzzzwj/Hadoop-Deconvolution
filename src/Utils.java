import org.apache.hadoop.io.IntWritable;

import java.io.*;
import java.util.ArrayList;

class Record{
    String descriptor;
    int[] value;
}

public class Utils {
    private static final char[] hexCode = "0123456789ABCDEF".toCharArray();

    public static String printHexBinary(byte[] val){
        StringBuilder sb = new StringBuilder(val.length * 2);
        for(byte b : val){
            sb.append(hexCode[(b >> 4) & 0xF]);
            sb.append(hexCode[b & 0xF]);
            sb.append(" ");
        }
        return sb.toString();
    }

    public static int bytes2int(byte[] value, int offset){
        return bytes2int(value, offset, 32);
    }

    public static int bytes2int(byte[] value, int offset, int bits){
        int res=0;
        switch(bits){
            case 8:res = (value[offset]); break;
            case 16:res = ((value[offset] & 0xFF) << 8) | (value[offset+1] & 0xFF); break;
            case 32:res = ((value[offset] & 0xFF) << 24) | ((value[offset+1] & 0xFF) << 16) | ((value[offset+2] & 0xFF) << 8) | (value[offset+3] & 0xFF); break;
            default:res = 0; break;
        }
        return res;
    }

    public static byte[] int2bytes(int value, int bits){
        byte[] res;
        switch(bits){
            case 8:res = new byte[]{(byte)value}; break;
            case 16:res = new byte[]{(byte)((value >> 8) & 0xFF), (byte)value}; break;
            case 32:res = new byte[]{(byte)((value >> 24) & 0xFF), (byte)((value >> 16) & 0xFF), (byte)((value >> 8) & 0xFF), (byte)value}; break;
            default:res = new byte[]{0}; break;
        }
        return res;
    }

    public static double[] ints2doubles(int[] value){
        double[] res = new double[value.length];
        for(int i = 0; i < res.length; i++){
            res[i] = value[i];
        }
        return res;
    }

    public static int[] doubles2ints(double[] value){
        int[] res = new int[value.length];
        for(int i = 0; i < res.length; i++){
            res[i] = (int)value[i];
        }
        return res;
    }

    public static int[] subgraph(int[] src, int x, int y, int startx, int starty, int subx, int suby){
//        System.out.println("x:" + x + "\ty:" + y + "\tstartx:" + startx + "\tstarty:" + starty + "\tsubx:" + subx + "\tsuby:" + suby);
        int[] res = new int[subx * suby];
        for (int j = starty; j < starty + suby; j++){
            for(int i = startx; i < startx + subx; i++){
//                System.out.println(i + " " + j);
                res[(j - starty) * subx + (i - startx)] = src[j * x + i];
            }
        }
        return res;
    }

    private static int getSubgraphstart(int idx, int x, int factor, int padding) {
        if(factor == 1)
            return 0;
        if(idx == 0)
            return 0;
        else
            return idx * x / factor - padding;
    }

    public static int[] getCutgraphrange(int idx, int x, int factor, int padding) {
        if(idx == 0)
            return new int[]{0, x - padding};
        else if(idx == factor - 1)
            return new int[]{padding, x};
        else
            return new int[]{padding, x - padding};
    }

    private static int getSubgraphlen(int idx, int x, int factor, int padding) {
        if(factor == 1)
            return x;

        if(idx == 0 || idx == factor - 1)
            return x / factor + padding;
        else
            return x / factor + 2 * padding;
    }

    public static ArrayList<Record> all_subgraph(int[] src, int x, int y, int factor, int padding){
        ArrayList<Record> list = new ArrayList<>();
        for(int i = 0; i < factor; i++){
            int subx = getSubgraphlen(i, x, factor, padding);
            int startx = getSubgraphstart(i, x, factor, padding);
            for(int j = 0; j < factor; j++){
                int suby = getSubgraphlen(j, y, factor, padding);
                int starty = getSubgraphstart(j, y, factor, padding);
                Record r = new Record();
                r.value = subgraph(src, x, y, startx, starty, subx, suby);
                r.descriptor = i + " " + subx + " " + j + " " + suby + " " + padding;
                list.add(r);
            }
        }
        return list;
    }

    public static int[] bytes2ints(byte[] value, int len){
        int[] res = new int[value.length / len];
        for(int i = 0; i < res.length; i++){
            res[i] = bytes2int(value, i * len, len * 8);
        }
        return res;
    }

    public static byte[] ints2bytes(int[] value, int len){
        byte[] res = new byte[value.length * len];
        for(int i = 0; i < value.length; i++){
            byte[] tmp = int2bytes(value[i], len * 8);
            System.arraycopy(tmp, 0, res, i * len, tmp.length);
        }
        return res;
    }

    public static int[] listappend(int[] src1, int[] src2){
        int[] res = new int[src1.length + src2.length];
        for(int i = 0; i < src1.length; i++){
            res[i] = src1[i];
        }
        for(int i = 0; i < src2.length; i++){
            res[i + src1.length] = src2[i];
        }
        return res;
    }

    public static String getFilename(String[] keywords){
        String filename = keywords[1] + "-" + keywords[3] + "-" + keywords[5];
        return filename;
    }

    public static int[] getPSF(InputStream in, int[] shape) throws IOException {
        byte[] head = new byte[16];
        in.read(head, 0, head.length);
        shape[0] = bytes2int(head, 0, 32);
        shape[1] = bytes2int(head, 4, 32);
        shape[2] = bytes2int(head, 8, 32);
        int bits = bytes2int(head, 12, 32);

        byte[] byte_buffer = new byte[shape[0] * shape[1] * shape[2] * bits / 8];
        in.read(byte_buffer, 0, byte_buffer.length);
        int[] int_buffer = bytes2ints(byte_buffer, bits / 8);

        in.close();

        return int_buffer;
    }
}
