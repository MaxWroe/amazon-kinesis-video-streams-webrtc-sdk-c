#include "Samples.h"
#include <gst/gst.h>
#include <gst/app/gstappsink.h>

extern PSampleConfiguration gSampleConfiguration;

// Global objects for simplified access
GstElement* senderPipeline = NULL;
GstElement* gVideoEncoder = NULL;

GstFlowReturn on_new_sample(GstElement* sink, gpointer data)
{
    GstBuffer* buffer;
    BOOL isDroppable, delta;
    GstFlowReturn ret = GST_FLOW_OK;
    GstSample* sample = NULL;
    GstMapInfo info;
    GstSegment* segment;
    GstClockTime buf_pts;
    Frame frame;
    STATUS status;
    PSampleConfiguration pSampleConfiguration = (PSampleConfiguration) data;
    PSampleStreamingSession pSampleStreamingSession = NULL;
    PRtcRtpTransceiver pRtcRtpTransceiver = NULL;
    UINT32 i;

    if (pSampleConfiguration == NULL) {
        return GST_FLOW_ERROR;
    }

    info.data = NULL;
    sample = gst_app_sink_pull_sample(GST_APP_SINK(sink));

    buffer = gst_sample_get_buffer(sample);
    
    // Dropping logic for bad buffers
    isDroppable = GST_BUFFER_FLAG_IS_SET(buffer, GST_BUFFER_FLAG_CORRUPTED) ||
                  GST_BUFFER_FLAG_IS_SET(buffer, GST_BUFFER_FLAG_DECODE_ONLY) ||
                  (GST_BUFFER_FLAGS(buffer) == GST_BUFFER_FLAG_DISCONT) ||
                  (GST_BUFFER_FLAG_IS_SET(buffer, GST_BUFFER_FLAG_DISCONT) && GST_BUFFER_FLAG_IS_SET(buffer, GST_BUFFER_FLAG_DELTA_UNIT)) ||
                  !GST_BUFFER_PTS_IS_VALID(buffer);

    if (!isDroppable) {
        delta = GST_BUFFER_FLAG_IS_SET(buffer, GST_BUFFER_FLAG_DELTA_UNIT);
        frame.flags = delta ? FRAME_FLAG_NONE : FRAME_FLAG_KEY_FRAME;

        // Convert timestamps
        segment = gst_sample_get_segment(sample);
        buf_pts = gst_segment_to_running_time(segment, GST_FORMAT_TIME, buffer->pts);
        
        if (!GST_CLOCK_TIME_IS_VALID(buf_pts)) {
             // Invalid timestamp, discard
             goto CleanUp;
        }

        if (!(gst_buffer_map(buffer, &info, GST_MAP_READ))) {
            goto CleanUp;
        }

        // Setup Frame metadata
        frame.trackId = DEFAULT_VIDEO_TRACK_ID;
        frame.duration = 0;
        frame.version = FRAME_CURRENT_VERSION;
        frame.size = (UINT32) info.size;
        frame.frameData = (PBYTE) info.data;

        MUTEX_LOCK(pSampleConfiguration->streamingSessionListReadLock);
        
        // Iterate through active WebRTC sessions
        for (i = 0; i < pSampleConfiguration->streamingSessionCount; ++i) {
            pSampleStreamingSession = pSampleConfiguration->sampleStreamingSessionList[i];
            
            // Increment frame index
            frame.index = (UINT32) ATOMIC_INCREMENT(&pSampleStreamingSession->frameIndex);

            // Set transceiver to Video
            pRtcRtpTransceiver = pSampleStreamingSession->pVideoRtcRtpTransceiver;
            
            // Timestamp logic
            frame.presentationTs = pSampleStreamingSession->videoTimestamp;
            frame.decodingTs = frame.presentationTs;
            
            // Assume 15 FPS (approx 66ms per frame) based on your python script
            // 66666666 ns = 66ms. You can adjust this if you change framerate.
            pSampleStreamingSession->videoTimestamp += 66666666; 

            // Send Frame to KVS WebRTC
            status = writeFrame(pRtcRtpTransceiver, &frame);
            
            if (status == STATUS_SUCCESS && pSampleStreamingSession->firstFrame) {
                PROFILE_WITH_START_TIME(pSampleStreamingSession->offerReceiveTime, "Time to first frame");
                pSampleStreamingSession->firstFrame = FALSE;
            }
        }
        MUTEX_UNLOCK(pSampleConfiguration->streamingSessionListReadLock);
    }

CleanUp:
    if (info.data != NULL) {
        gst_buffer_unmap(buffer, &info);
    }
    if (sample != NULL) {
        gst_sample_unref(sample);
    }
    if (ATOMIC_LOAD_BOOL(&pSampleConfiguration->appTerminateFlag)) {
        ret = GST_FLOW_EOS;
    }

    return ret;
}

PVOID sendGstreamerAudioVideo(PVOID args)
{
    STATUS retStatus = STATUS_SUCCESS;
    GstElement *appsinkVideo = NULL;
    GstBus* bus;
    GstMessage* msg;
    GError* error = NULL;
    PSampleConfiguration pSampleConfiguration = (PSampleConfiguration) args;

    if (pSampleConfiguration == NULL) {
        return (PVOID) (ULONG_PTR) STATUS_NULL_ARG;
    }

    // -------------------------------------------------------------------------
    // RASPBERRY PI VIDEO PIPELINE (Rotated 90 deg + Hardware Encoding)
    // -------------------------------------------------------------------------
    // 1. libcamerasrc: Gets video from Pi Camera (replaces libcamera-vid)
    // 2. caps: Sets input res to 1280x720 @ 15fps
    // 3. videoconvert: Ensures color format compatibility
    // 4. videoflip method=clockwise: Rotates 90 degrees (Output becomes 720x1280)
    // 5. v4l2h264enc: Raspberry Pi Hardware Encoder
    // 6. h264parse: Parses stream for WebRTC compatibility
    // -------------------------------------------------------------------------
    
    senderPipeline = gst_parse_launch(
        "libcamerasrc ! "
        "video/x-raw,width=1280,height=720,framerate=15/1 ! "
        "videoconvert ! "
        "videoflip method=clockwise ! "
        "video/x-raw,width=720,height=1280 ! "
        "v4l2h264enc name=sampleVideoEncoder extra-controls=\"controls,video_bitrate=1000000\" ! "
        "h264parse ! " 
        "video/x-h264,stream-format=byte-stream,alignment=au,profile=baseline ! "
        "appsink sync=TRUE emit-signals=TRUE name=appsink-video",
        &error);

    if (senderPipeline == NULL) {
        DLOGE("[KVS Master] Pipeline creation failed: %s", error->message);
        g_clear_error(&error);
        return (PVOID) (ULONG_PTR) STATUS_INTERNAL_ERROR;
    }

    // Cache the encoder and appsink for later use
    gVideoEncoder = gst_bin_get_by_name(GST_BIN(senderPipeline), "sampleVideoEncoder");
    appsinkVideo = gst_bin_get_by_name(GST_BIN(senderPipeline), "appsink-video");

    if (appsinkVideo == NULL) {
        DLOGE("[KVS Master] Cannot find appsink-video");
        goto CleanUp;
    }

    // Connect the callback
    g_signal_connect(appsinkVideo, "new-sample", G_CALLBACK(on_new_sample), (gpointer) pSampleConfiguration);
    
    // Start Pipeline
    gst_element_set_state(senderPipeline, GST_STATE_PLAYING);

    // Watch bus for errors
    bus = gst_element_get_bus(senderPipeline);
    msg = gst_bus_timed_pop_filtered(bus, GST_CLOCK_TIME_NONE, GST_MESSAGE_ERROR | GST_MESSAGE_EOS);

    // Free resources when done
    if (msg != NULL) gst_message_unref(msg);
    if (bus != NULL) gst_object_unref(bus);
    
    gst_element_set_state(senderPipeline, GST_STATE_NULL);
    gst_object_unref(senderPipeline);
    if (appsinkVideo != NULL) gst_object_unref(appsinkVideo);
    if (gVideoEncoder != NULL) gst_object_unref(gVideoEncoder);

CleanUp:
    if (error != NULL) {
        DLOGE("[KVS Master] Error: %s", error->message);
        g_clear_error(&error);
    }

    return (PVOID) (ULONG_PTR) retStatus;
}

INT32 main(INT32 argc, CHAR* argv[])
{
    STATUS retStatus = STATUS_SUCCESS;
    PSampleConfiguration pSampleConfiguration = NULL;
    PCHAR pChannelName;

    SET_INSTRUMENTED_ALLOCATORS();
    UINT32 logLevel = setLogLevel();

    signal(SIGINT, sigintHandler);

#ifdef IOT_CORE_ENABLE_CREDENTIALS
    pChannelName = argc > 1 ? argv[1] : GETENV(IOT_CORE_THING_NAME);
    if (pChannelName == NULL) {
        DLOGE("[KVS Master] AWS_IOT_CORE_THING_NAME must be set");
        return EXIT_FAILURE;
    }
#else
    pChannelName = argc > 1 ? argv[1] : SAMPLE_CHANNEL_NAME;
#endif

    // Initialize Configuration
    CHK_STATUS(createSampleConfiguration(pChannelName, SIGNALING_CHANNEL_ROLE_TYPE_MASTER, TRUE, TRUE, logLevel, &pSampleConfiguration));

    // Force Pi-Specific Settings
    pSampleConfiguration->videoSource = sendGstreamerAudioVideo;
    pSampleConfiguration->mediaType = SAMPLE_STREAMING_VIDEO_ONLY;
    pSampleConfiguration->audioCodec = RTC_CODEC_OPUS; // Unused but required by init
    pSampleConfiguration->videoCodec = RTC_CODEC_H264_PROFILE_42E01F_LEVEL_ASYMMETRY_ALLOWED_PACKETIZATION_MODE;
    pSampleConfiguration->srcType = DEVICE_SOURCE; 

#ifdef ENABLE_DATA_CHANNEL
    pSampleConfiguration->onDataChannel = onDataChannel;
#endif
    pSampleConfiguration->customData = (UINT64) pSampleConfiguration;

    // Initialize GStreamer
    gst_init(&argc, &argv);
    DLOGI("[KVS Master] GStreamer Initialized");
    DLOGI("[KVS Master] Starting Video-Only Stream (Hardware Encoded, 90deg Rotated)");

    // Initialize KVS WebRTC
    CHK_STATUS(initKvsWebRtc());
    CHK_STATUS(initSignaling(pSampleConfiguration, SAMPLE_MASTER_CLIENT_ID));

    // Block until interrupted
    CHK_STATUS(sessionCleanupWait(pSampleConfiguration));

CleanUp:
    if (retStatus != STATUS_SUCCESS) {
        DLOGE("[KVS Master] Terminated with status code 0x%08x", retStatus);
    }

    // Teardown
    if (pSampleConfiguration != NULL) {
        ATOMIC_STORE_BOOL(&pSampleConfiguration->appTerminateFlag, TRUE);

        if (pSampleConfiguration->mediaSenderTid != INVALID_TID_VALUE) {
            THREAD_JOIN(pSampleConfiguration->mediaSenderTid, NULL);
        }

        if (pSampleConfiguration->enableFileLogging) {
            freeFileLogger();
        }
        
        freeSignalingClient(&pSampleConfiguration->signalingClientHandle);
        freeSampleConfiguration(&pSampleConfiguration);
    }
    
    RESET_INSTRUMENTED_ALLOCATORS();
    return STATUS_FAILED(retStatus) ? EXIT_FAILURE : EXIT_SUCCESS;
}