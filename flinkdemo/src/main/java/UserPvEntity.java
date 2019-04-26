/**
 * @Auther: jiashaoguan
 * @Date: 2019/4/12
 * @Description:
 * @return
 */
public class UserPvEntity {
    private long time;
    private String userId;
    private long pvcount;

    public UserPvEntity(long time, String userId, long pvcount) {
        this.time = time;
        this.userId = userId;
        this.pvcount = pvcount;
    }

    public long getTime() {
        return time;
    }

    public void setTime(long time) {
        this.time = time;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public long getPvcount() {
        return pvcount;
    }

    public void setPvcount(long pvcount) {
        this.pvcount = pvcount;
    }
}
