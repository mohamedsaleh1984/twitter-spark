import java.util.ArrayList;
import java.util.Date;

public class Root{
    public Data data;
    public Includes includes;
    public ArrayList<MatchingRule> matching_rules;
    
    public class Data{
        public String author_id;
        public Date created_at;
        public String id;
        public String lang;
        public ArrayList<ReferencedTweet> referenced_tweets;
        public String text;
    }

    public class Includes{
        public ArrayList<User> users;
    }

    public class MatchingRule{
        public String id;
        public String tag;
    }

    public class ReferencedTweet{
        public String type;
        public String id;
    }
    
    public class User{
        public String id;
        public String name;
        public String username;
        public String location;
    }
}

