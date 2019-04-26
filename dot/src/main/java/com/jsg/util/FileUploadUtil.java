package com.jsg.util;

import java.io.File;
import java.io.FileOutputStream;

public class FileUploadUtil {

    public static void uploadFileToLocal(byte[] file, String filePath, String fileName) throws Exception{

        File targetFile = new File(filePath);
        if(!targetFile.exists()){
            targetFile.mkdirs();
        }
        FileOutputStream out = new FileOutputStream(filePath+fileName);
        out.write(file);
        out.flush();
        out.close();
    }
}
