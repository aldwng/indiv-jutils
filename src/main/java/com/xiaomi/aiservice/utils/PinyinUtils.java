package com.xiaomi.aiservice.utils;

import com.google.common.base.Throwables;
import net.sourceforge.pinyin4j.PinyinHelper;
import net.sourceforge.pinyin4j.format.HanyuPinyinCaseType;
import net.sourceforge.pinyin4j.format.HanyuPinyinOutputFormat;
import net.sourceforge.pinyin4j.format.HanyuPinyinToneType;
import net.sourceforge.pinyin4j.format.HanyuPinyinVCharType;
import net.sourceforge.pinyin4j.format.exception.BadHanyuPinyinOutputFormatCombination;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.regex.Pattern;

/**
 * @author wanglingda@xiaomi.com
 */

public class PinyinUtils {

	private static final Logger LOGGER = LoggerFactory.getLogger(PinyinUtils.class);
	private static final Pattern CH_PATTERN = Pattern.compile("[\\u4e00-\\u9fa5]+");
	private static final HanyuPinyinOutputFormat NAIVE_FORMAT = getNaiveFormat();

	private static HanyuPinyinOutputFormat getNaiveFormat() {
		HanyuPinyinOutputFormat format = new HanyuPinyinOutputFormat();
		format.setCaseType(HanyuPinyinCaseType.LOWERCASE);
		format.setToneType(HanyuPinyinToneType.WITHOUT_TONE);
		format.setVCharType(HanyuPinyinVCharType.WITH_V);
		return format;
	}

	public static String getNaivePinyin(String str) {
		if (StringUtils.isBlank(str)) {
			return str;
		}
		char[] chars = str.trim().toCharArray();
		try {
			StringBuilder pinyinBuilder = new StringBuilder();
			for (char char0 : chars) {
				if (CH_PATTERN.matcher(Character.toString(char0)).find()) {
					String[] pinyins = PinyinHelper.toHanyuPinyinStringArray(char0, NAIVE_FORMAT);
					if (pinyins.length > 0) {
						pinyinBuilder.append(pinyins[0]);
					}
				} else {
					pinyinBuilder.append(char0);
				}
			}
			return pinyinBuilder.toString();
		} catch (BadHanyuPinyinOutputFormatCombination e) {
			LOGGER.error("trans string {} to pinyin failed due to {}", str, Throwables.getStackTraceAsString(e));
		}
		return str;
	}
}
