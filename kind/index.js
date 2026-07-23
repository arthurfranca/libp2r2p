export const METADATA = 0
export const TEXT_NOTE = 1
export const RECOMMEND_RELAY = 2
export const FOLLOWS = 3
export const ENCRYPTED_DIRECT_MESSAGE = 4
export const DELETION = 5
export const REPOST = 6
export const REACTION = 7
export const BADGE_AWARD = 8
export const SEAL = 13
export const PRIVATE_DIRECT_MESSAGE = 14
export const GENERIC_REPOST = 16
export const PICTURE = 20
export const VIDEO = 21
export const SHORT_VIDEO = 22
export const CHANNEL_CREATE = 40
export const CHANNEL_METADATA = 41
export const CHANNEL_MESSAGE = 42
export const CHANNEL_HIDE_MESSAGE = 43
export const CHANNEL_MUTE_USER = 44
export const REGULAR_CUSTOM_APP_DATA = 78
export const PERSONAL_COPY = 1006
export const OPEN_TIMESTAMPS = 1040
export const GIFT_WRAP = 1059
export const FILE_METADATA = 1063
export const COMMENT = 1111
export const VOICE_MESSAGE = 1222
export const VOICE_MESSAGE_REPLY = 1244
export const LIVE_CHAT_MESSAGE = 1311
export const PROBLEM_TRACKER = 1971
export const REPORT = 1984
export const LABEL = 1985
export const PRIVATE_CHANNEL_BROADCAST = 3560
export const COMMUNITY_POST_APPROVAL = 4550
export const JOB_REQUEST = 5999
export const JOB_RESULT = 6999
export const JOB_FEEDBACK = 7000
export const ZAP_GOAL = 9041
export const ZAP_REQUEST = 9734
export const ZAP = 9735
export const HIGHLIGHTS = 9802
export const MUTE_LIST = 10000
export const PINNED_NOTES = 10001
export const READ_WRITE_RELAYS = 10002
export const BOOKMARKS = 10003
export const COMMUNITIES = 10004
export const PUBLIC_CHATS = 10005
export const BLOCKED_RELAYS = 10006
export const SEARCH_RELAYS = 10007
export const SIMPLE_GROUPS = 10009
export const RELAY_FEEDS = 10012
export const INTERESTS = 10015
export const MEDIA_FOLLOWS = 10020
export const EMOJIS = 10030
export const DM_RELAYS = 10050
export const FILE_SERVER_PREFERENCE = 10096
export const GOOD_WIKI_AUTHORS = 10101
export const GOOD_WIKI_RELAYS = 10102
export const NWC_WALLET_INFO = 13194
export const LIGHTNING_PUB_RPC = 21000
export const AUTH = 22242
export const NWC_WALLET_REQUEST = 23194
export const NWC_WALLET_RESPONSE = 23195
export const SIGNER_RPC = 24133
export const HTTP_AUTH = 27235
export const NWT = 27519
export const FOLLOW_SET = 30000
export const LIST = 30001
export const RELAY_SET = 30002
export const BOOKMARK_SET = 30003
export const CURATION_SET = 30004
export const VIDEO_CURATION_SET = 30005
export const PICTURE_CURATION_SET = 30006
export const KIND_MUTE_SET = 30007
export const PROFILE_BADGES = 30008
export const BADGE_DEFINITION = 30009
export const INTEREST_SET = 30015
export const CREATE_OR_UPDATE_STALL = 30017
export const CREATE_OR_UPDATE_PRODUCT = 30018
export const LONG_FORM_CONTENT = 30023
export const DRAFT_LONG = 30024
export const EMOJI_SET = 30030
export const RELEASE_ARTIFACT_SET = 30063
export const CUSTOM_APP_DATA = 30078
export const APP_CURATION_SET = 30267
export const LIVE_EVENT = 30311
export const USER_STATUSES = 30315
export const I_TAG_TRUSTED_ASSERTION = 30385
export const CLASSIFIED_LISTING = 30402
export const DRAFT_CLASSIFIED_LISTING = 30403
export const DATE_BASED_CALENDAR_EVENT = 31922
export const TIME_BASED_CALENDAR_EVENT = 31923
export const CALENDAR = 31924
export const CALENDAR_EVENT_RSVP = 31925
export const HANDLER_RECOMMENDATION = 31989
export const HANDLER_INFORMATION = 31990
export const EDITABLE_VIDEO = 34235
export const EDITABLE_SHORT_VIDEO = 34236
export const COMMUNITY_DEFINITION = 34550
export const BINARY_DATA_CHUNK = 34601
export const MAIN_SITE_MANIFEST = 35128
export const NEXT_SITE_MANIFEST = 35129
export const DRAFT_SITE_MANIFEST = 35130
export const STARTER_PACK = 39089
export const MEDIA_STARTER_PACK = 39092

const classifications = ['regular', 'replaceable', 'ephemeral', 'addressable']

function isValidKind (kind) {
  return Number.isInteger(kind) && kind >= 0 && kind <= 0xffff
}

export function isRegularKind (kind) {
  return isValidKind(kind) && (
    (kind >= 1000 && kind < 10000) ||
    (kind >= 4 && kind < 45) ||
    kind === 1 || kind === 2
  )
}

export function isReplaceableKind (kind) {
  return isValidKind(kind) && (kind === 0 || kind === 3 || (kind >= 10000 && kind < 20000))
}

export function isEphemeralKind (kind) {
  return isValidKind(kind) && kind >= 20000 && kind < 30000
}

export function isAddressableKind (kind) {
  return isValidKind(kind) && kind >= 30000 && kind < 40000
}

export function classifyKind (kind) {
  const predicates = [isRegularKind, isReplaceableKind, isEphemeralKind, isAddressableKind]
  return classifications.filter((_, index) => predicates[index](kind))
}

export const eventKinds = /* @__PURE__ */ Object.freeze({
  METADATA,
  TEXT_NOTE,
  RECOMMEND_RELAY,
  FOLLOWS,
  ENCRYPTED_DIRECT_MESSAGE,
  DELETION,
  REPOST,
  REACTION,
  BADGE_AWARD,
  SEAL,
  PRIVATE_DIRECT_MESSAGE,
  GENERIC_REPOST,
  PICTURE,
  VIDEO,
  SHORT_VIDEO,
  CHANNEL_CREATE,
  CHANNEL_METADATA,
  CHANNEL_MESSAGE,
  CHANNEL_HIDE_MESSAGE,
  CHANNEL_MUTE_USER,
  REGULAR_CUSTOM_APP_DATA,
  PERSONAL_COPY,
  OPEN_TIMESTAMPS,
  GIFT_WRAP,
  FILE_METADATA,
  COMMENT,
  VOICE_MESSAGE,
  VOICE_MESSAGE_REPLY,
  LIVE_CHAT_MESSAGE,
  PROBLEM_TRACKER,
  REPORT,
  LABEL,
  PRIVATE_CHANNEL_BROADCAST,
  COMMUNITY_POST_APPROVAL,
  JOB_REQUEST,
  JOB_RESULT,
  JOB_FEEDBACK,
  ZAP_GOAL,
  ZAP_REQUEST,
  ZAP,
  HIGHLIGHTS,
  MUTE_LIST,
  PINNED_NOTES,
  READ_WRITE_RELAYS,
  BOOKMARKS,
  COMMUNITIES,
  PUBLIC_CHATS,
  BLOCKED_RELAYS,
  SEARCH_RELAYS,
  SIMPLE_GROUPS,
  RELAY_FEEDS,
  INTERESTS,
  MEDIA_FOLLOWS,
  EMOJIS,
  DM_RELAYS,
  FILE_SERVER_PREFERENCE,
  GOOD_WIKI_AUTHORS,
  GOOD_WIKI_RELAYS,
  NWC_WALLET_INFO,
  LIGHTNING_PUB_RPC,
  AUTH,
  NWC_WALLET_REQUEST,
  NWC_WALLET_RESPONSE,
  SIGNER_RPC,
  HTTP_AUTH,
  NWT,
  FOLLOW_SET,
  LIST,
  RELAY_SET,
  BOOKMARK_SET,
  CURATION_SET,
  VIDEO_CURATION_SET,
  PICTURE_CURATION_SET,
  KIND_MUTE_SET,
  PROFILE_BADGES,
  BADGE_DEFINITION,
  INTEREST_SET,
  CREATE_OR_UPDATE_STALL,
  CREATE_OR_UPDATE_PRODUCT,
  LONG_FORM_CONTENT,
  DRAFT_LONG,
  EMOJI_SET,
  RELEASE_ARTIFACT_SET,
  CUSTOM_APP_DATA,
  APP_CURATION_SET,
  LIVE_EVENT,
  USER_STATUSES,
  I_TAG_TRUSTED_ASSERTION,
  CLASSIFIED_LISTING,
  DRAFT_CLASSIFIED_LISTING,
  DATE_BASED_CALENDAR_EVENT,
  TIME_BASED_CALENDAR_EVENT,
  CALENDAR,
  CALENDAR_EVENT_RSVP,
  HANDLER_RECOMMENDATION,
  HANDLER_INFORMATION,
  EDITABLE_VIDEO,
  EDITABLE_SHORT_VIDEO,
  COMMUNITY_DEFINITION,
  BINARY_DATA_CHUNK,
  MAIN_SITE_MANIFEST,
  NEXT_SITE_MANIFEST,
  DRAFT_SITE_MANIFEST,
  STARTER_PACK,
  MEDIA_STARTER_PACK
})
