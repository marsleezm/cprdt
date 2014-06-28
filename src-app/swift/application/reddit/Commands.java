/*****************************************************************************
 * Copyright 2011-2012 INRIA
 * Copyright 2011-2012 Universidade Nova de Lisboa
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *****************************************************************************/
package swift.application.reddit;

public enum Commands {
    LOGIN, // ;username;password
    LOGOUT,
    READ_LINKS, // ;subreddit;order Read links of a subreddit (in a certain order)
    READ_COMMENTS, // ;link_id;order
    POST_LINK, // ;link_id;subreddit;date;title;url;selftext (link_id is optional)
    POST_COMMENT, // ;parent_comment;date,content
    VOTE_LINK, // ;link_id;vote_direction
    VOTE_COMMENT, // ;comment_id;vote_direction
    CREATE_SUBREDDIT // ;subreddit
}
